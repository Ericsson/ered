-module(ered_command).

%% Command formatting and related functions.

-export([convert_to/1,
         get_data/1,
         get_response_class/1,
         check_result/1,
         add_asking/2,
         fix_ask_reply/2]).

-export_type([command/0,
              redis_command/0,
              raw_command/0,
              raw_command_pipeline/0,
              response_class/0]).
%%%===================================================================
%%% Definitions
%%%===================================================================

%% The difference between a pipeline with length 1 and a with 'single'
%% command is that the pipeline will return the result in a list. It
%% would be nice to skip the 'single' and treat it as a pipeline with
%% N 1 and just unpack it from the list in the calling process. The
%% problem is when returning the result with a callback function in
%% async. It could be handled by wrapping the callback fun in another
%% fun that unpacks it. Not sure what is worse..
-type redis_command() ::
        {redis_command, {single, response_class()}, binary()} |
        {redis_command, {pipeline, [response_class()]}, binary()}.


-type raw_command() :: [binary()].
-type raw_command_pipeline() :: [raw_command()].
-type command() :: redis_command() | raw_command() | raw_command_pipeline().
-type ok_result() :: {ok, ered_parser:parse_result()}.
-type response_class() :: {binary(), non_neg_integer()} | normal.

%%%===================================================================
%%% API
%%%===================================================================

%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-spec convert_to(command()) -> redis_command().
%%
%% Convert a command or list of commands to RESP format
%% (REdis Serialization Protocol).
%% Commands are given as a list of binaries where the first element
%% is the command and the rest are arguments.
%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
convert_to(Command = {redis_command, _, _}) ->
    Command;

convert_to(Data) when is_binary(Data) ->
    convert_to([Data]);

convert_to([]) ->
    error({badarg, []});

convert_to(RawCommands = [E|_]) when is_list(E) ->
    {Bin, Classes} =
        lists:foldl(fun(RawCommand, {BinAcc, ClassAcc}) ->
                            command_to_bin(RawCommand, BinAcc, ClassAcc)
                    end, {<<>>, []}, RawCommands),
    {redis_command, {pipeline, lists:reverse(Classes)}, Bin};

convert_to(RawCommand) ->
    {Command, [Class]} = command_to_bin(RawCommand, <<>>, []),
    {redis_command, {single, Class}, Command}.

command_to_bin(RawCommand, BinAcc, ClassAcc) ->
    Len = integer_to_binary(length(RawCommand)),
    Header = <<BinAcc/binary, "*", Len/binary, "\r\n">>,
    Bin = lists:foldl(fun(Arg, Acc) ->
                              Size = integer_to_binary(byte_size(Arg)),
                              <<Acc/binary, $$, Size/binary, "\r\n", Arg/binary, "\r\n">>
                      end, Header, RawCommand),
    Class = resp_class(RawCommand),
    {Bin, [Class | ClassAcc]}.

%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-spec get_data(redis_command()) -> binary().
%%
%% Returns the command binary data to send to the socket.
%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
get_data({redis_command, _, Data}) ->
    Data.

%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-spec get_response_class(redis_command()) ->
          response_class() | [response_class()].
%%
%% Returns a classification of the command(s) which is used for
%% mapping responses to the commands. Special handling is needed for
%% some pubsub commands.
%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
get_response_class({redis_command, {single, Class}, _}) -> Class;
get_response_class({redis_command, {pipeline, Classes}, _}) ->
    Classes.

%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-spec resp_class(raw_command()) -> response_class().
%%
%% Given a raw command (list of binaries), returns a classification
%% which can be used to interpret the response(s) from Redis,
%% particularly for pubsub commands that don't return anything but
%% expect certain push messages to indicate success.
%%
%% If the command name ends in "subscribe", returns a tuple
%% {CommandName, NumChannels}. Returns 'normal' otherwise.
%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
resp_class([CmdName | Args]) ->
    Len = byte_size(CmdName),
    if Len < 9; Len > 12 ->
            %% Quick path for most commands.
            %% Shorter than "subscribe" or longer than "punsubscribe".
            normal;
       true ->
            case binary:at(CmdName, Len - 2) of
                B when B =:= $b; B =:= $B ->
                    %% The B in SUBSCRIBE is at the right position (the
                    %% penultimate letter). This check eliminates all regular
                    %% commands of length 9-12 except the ones that end with
                    %% "subscribe". Now do the slow case-insensitive check to
                    %% be sure.
                    case string:lowercase(CmdName) of
                        <<_:(Len - 9)/binary, "subscribe">> = LowercaseCmd ->
                            {LowercaseCmd, length(Args)};
                        _ ->
                            normal
                    end;
                _ ->
                    normal
            end
    end;
resp_class(_) ->
    normal.

%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-spec check_result(ok_result() | any()) ->
          normal |
          try_again |
          cluster_down |
          {moved | ask, ered_lib:addr()}.
%%
%% Check for results that need special handling.
%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
check_result({ok, Result}) when is_list(Result) ->
    check_for_special_reply(Result);
check_result({ok, Result}) ->
    check_for_special_reply([Result]);
check_result(_) ->
    normal.

check_for_special_reply([]) ->
    normal;
check_for_special_reply([Head|Tail]) ->
    case Head of
        {error, <<"TRYAGAIN ", _/binary>>} ->
            try_again;
        {error, <<"MOVED ", Bin/binary>>} ->
            {moved, parse_host_and_port(Bin)};
        {error, <<"ASK ", Bin/binary>>} ->
            {ask, parse_host_and_port(Bin)};
        {error, <<"CLUSTERDOWN ", _/binary>>} ->
            cluster_down;
        _ ->
            check_for_special_reply(Tail)
    end.

parse_host_and_port(Bin) ->
    %% looks like <<"MOVED 14039 127.0.0.1:30006">> or <<"ASK 15118 127.0.0.1:30002">>
    [_Slot, AddrBin] = binary:split(Bin, <<" ">>),
    %% Use trailing, IPv6 address can contain ':'
    [Host, Port] = string:split(AddrBin, <<":">>, trailing),
    {binary_to_list(Host), binary_to_integer(Port)}.


%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-spec add_asking(ok_result(), redis_command()) -> redis_command().
%%
%% Add ASKING for commands that got an ASK error. The Redis result is needed
%% to filter out what commands to keep.
%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
add_asking(_, {redis_command, {single, Class}, Command}) ->
    AskingBin = <<"ASKING\r\n">>,
    Bin = <<AskingBin/binary, Command/binary>>,
    {redis_command, {pipeline, [normal, Class]}, Bin};

add_asking({ok, OldReplies}, {redis_command, {pipeline, Classes}, PipelineBin}) ->
    %% Split the concatenated binary back into individual commands,
    %% then pick only the ones with ASK redirection.
    Commands = split_pipeline_bin(PipelineBin),
    {AskCommands, AskClasses} = add_asking_pipeline(OldReplies, Commands, Classes),
    NewBin = lists:foldl(fun(C, Acc) -> <<Acc/binary, C/binary>> end, <<>>, AskCommands),
    {redis_command, {pipeline, AskClasses}, NewBin}.

add_asking_pipeline([], _, _) ->
    {[], []};
add_asking_pipeline([{error, <<"ASK ", _/binary>>} | Replies], [Command | Commands], [Class | Classes]) ->
    AskingBin = <<"ASKING\r\n">>,
    {RestCmds, RestClasses} = add_asking_pipeline(Replies, Commands, Classes),
    {[AskingBin, Command | RestCmds], [normal, Class | RestClasses]};
add_asking_pipeline([_ | Replies], [_ | Commands], [_ | Classes]) ->
    add_asking_pipeline(Replies, Commands, Classes).

%% Split a concatenated pipeline binary into individual RESP command binaries.
split_pipeline_bin(<<>>) ->
    [];
split_pipeline_bin(Bin) ->
    {Cmd, Rest} = split_one_command(Bin),
    [Cmd | split_pipeline_bin(Rest)].

split_one_command(<<"*", Rest/binary>>) ->
    {ArgcStr, Rest1} = read_until_crlf(Rest),
    Argc = binary_to_integer(ArgcStr),
    split_args(Argc, Rest1, <<"*", ArgcStr/binary, "\r\n">>).

split_args(0, Rest, Acc) ->
    {Acc, Rest};
split_args(N, <<"$", Rest/binary>>, Acc) ->
    {LenStr, Rest1} = read_until_crlf(Rest),
    Len = binary_to_integer(LenStr),
    <<ArgData:Len/binary, "\r\n", Rest2/binary>> = Rest1,
    split_args(N - 1, Rest2, <<Acc/binary, $$, LenStr/binary, "\r\n", ArgData/binary, "\r\n">>).

read_until_crlf(Bin) ->
    case binary:match(Bin, <<"\r\n">>) of
        {Pos, 2} ->
            <<Str:Pos/binary, "\r\n", Rest/binary>> = Bin,
            {Str, Rest}
    end.


%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-spec fix_ask_reply(ok_result(), ok_result()) -> ok_result().
%%
%% Remove the OK reply from the ASKING command in the result and insert
%% the other command replies into the old result, replacing the ASK errors
%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
fix_ask_reply({ok, OldReplies}, {ok, NewReplies}) when is_list(OldReplies) ->
    {ok, merge_ask_result(OldReplies, NewReplies)};
fix_ask_reply(_OldReply, {ok, [_AskOk, Reply]}) ->
    {ok, Reply};
fix_ask_reply(_, Other) ->
    Other.

merge_ask_result([], _) ->
    [];
merge_ask_result([{error, <<"ASK ", _/binary>>} | Replies1], [_AskOk, Reply | Replies2]) ->
    [Reply | merge_ask_result(Replies1, Replies2)];
merge_ask_result([Reply | Replies1], Replies2) ->
    [Reply | merge_ask_result(Replies1, Replies2)].
