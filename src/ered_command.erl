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
        {redis_command, single, binary()} |
        {redis_command, pipeline, [binary()]}.


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
    Commands = [command_to_bin(RawCommand) || RawCommand <- RawCommands],
    {redis_command, pipeline, Commands};

convert_to(RawCommand) ->
    Command = command_to_bin(RawCommand),
    {redis_command, single, Command}.

command_to_bin(RawCommand) ->
    Len = integer_to_list(length(RawCommand)),
    Elements = [["$", integer_to_list(size(Bin)), "\r\n", Bin, "\r\n"] || Bin <- RawCommand],
    %% Maybe this could be kept as an iolist?
    %% TODO profile this.
    %% Since this is copied around a bit between processes it might be cheaper to keep it as a binary
    %% since then it will be heap allocated if big. Just pure speculation..
    iolist_to_binary(["*", Len, "\r\n", Elements]).

%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-spec get_data(redis_command()) -> binary() | [binary()].
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
get_response_class({redis_command, single, Data}) ->
    resp_class(Data);
get_response_class({redis_command, pipeline, Data}) ->
    lists:map(fun resp_class/1, Data).

%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-spec resp_class(binary()) -> response_class().
%%
%% Given a RESP-formatted command, returns a classification which can
%% be used to interpret the response from Redis, particularily for
%% pubsub commands that don't return anything but expect certain push
%% messages to indicate success. The command must be in lowercase for
%% this to work.
%%
%% Returns, if the command is [*][un]subscribe, a tuple {pubsub,
%% CommandName, NumChannels}. Otherwise, 'normal' is returned.
%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
%% Single digit argc.
resp_class(<<"*", N, "\r\n$9\r\nsubscribe\r\n", _/binary>>) ->
    {<<"subscribe">>, N - $0 - 1};
resp_class(<<"*", N, "\r\n$10\r\n", X, "subscribe\r\n", _/binary>>)
  when X >= $a; X =< $z ->
    {<<X, "subscribe">>, N - $0 - 1};
resp_class(<<"*", N, "\r\n$11\r\nunsubscribe\r\n", _/binary>>) ->
    {<<"unsubscribe">>, N - $0 - 1};
resp_class(<<"*", N, "\r\n$12\r\n", X, "unsubscribe\r\n", _/binary>>)
  when X >= $a; X =< $z ->
    {<<X, "unsubscribe">>, N - $0 - 1};
%% Two-digit argc.
resp_class(<<"*", M, N, "\r\n$9\r\nsubscribe\r\n", _/binary>>) ->
    {<<"subscribe">>, (M - $0) * 10 + N - $0 - 1};
resp_class(<<"*", M, N, "\r\n$10\r\n", X, "subscribe\r\n", _/binary>>)
  when X >= $a; X =< $z ->
    {<<X, "subscribe">>, (M - $0) * 10 + N - $0 - 1};
resp_class(<<"*", M, N, "\r\n$11\r\nunsubscribe\r\n", _/binary>>) ->
    {<<"unsubscribe">>, (M - $0) * 10 + N - $0 - 1};
resp_class(<<"*", M, N, "\r\n$12\r\n", X, "unsubscribe\r\n", _/binary>>)
  when X >= $a; X =< $z ->
    {<<X, "unsubscribe">>, (M - $0) * 10 + N - $0 - 1};
%% Three-digit argc.
resp_class(<<"*", L, M, N, "\r\n$9\r\nsubscribe\r\n", _/binary>>) ->
    {<<"subscribe">>, (L - $0) * 100 + (M - $0) * 10 + N - $0 - 1};
resp_class(<<"*", L, M, N, "\r\n$10\r\n", X, "subscribe\r\n", _/binary>>)
  when X >= $a; X =< $z ->
    {<<X, "subscribe">>, (L - $0) * 100 + (M - $0) * 10 + N - $0 - 1};
resp_class(<<"*", L, M, N, "\r\n$11\r\nunsubscribe\r\n", _/binary>>) ->
    {<<"unsubscribe">>, (L - $0) * 100 + (M - $0) * 10 + N - $0 - 1};
resp_class(<<"*", L, M, N, "\r\n$12\r\n", X, "unsubscribe\r\n", _/binary>>)
  when X >= $a; X =< $z ->
    {<<X, "unsubscribe">>, (L - $0) * 100 + (M - $0) * 10 + N - $0 - 1};
%% Not a subscribe command.
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
add_asking(_, {redis_command, single, Command}) ->
    {redis_command, pipeline, [[<<"ASKING\r\n">>], Command]};

add_asking({ok, OldReplies}, {redis_command, pipeline, Commands}) ->
    %% Extract only the commands with ASK redirection. Even if all commands go to
    %% the same slot it does not have to be the same key. When using hash tags
    %% for instance.
    AskCommands = add_asking_pipeline(OldReplies, Commands),
    %% ASK commands + ASKING
    {redis_command, pipeline, AskCommands}.

add_asking_pipeline([], _) ->
    [];
add_asking_pipeline([{error, <<"ASK ", _/binary>>} |Replies], [Command |Commands]) ->
    [[<<"ASKING\r\n">>], Command | add_asking_pipeline(Replies, Commands)];
add_asking_pipeline([_ |Replies], [_ |Commands]) ->
    add_asking_pipeline(Replies, Commands).


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

