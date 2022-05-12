-module(redis_command).

-export([convert_to/1,
         get_count_and_data/1,
         check_result/1,
         add_asking/2,
         fix_ask_reply/2]).

-export_type([command/0,
              redis_command/0,
              raw_command/0,
              raw_command_pipeline/0]).
%%%===================================================================
%%% Definitions
%%%===================================================================

%% The difference between a pipeline with N 1 and a with 'single'
%% command is that the pipeline will return the result in a list. It
%% would be nice to skip the 'single' and treat it as a pipeline with
%% N 1 and just unpack it from the list in the calling process. The
%% problem is when returning the result with a callback function in
%% async. It could be handled by wrapping the callback fun in another
%% fun that unpacks it. Not sure what is worse..
-type redis_command() ::
        {redis_command, single, binary()} | %% single command
        {redis_command, non_neg_integer(), [iolist()]}. %% pipeline command


-type raw_command() :: [binary()].
-type raw_command_pipeline() :: [raw_command()].
-type command() :: redis_command() | raw_command() | raw_command_pipeline().
-type ok_result() :: {ok, redis_parser:parse_result()}.

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
    {redis_command, length(RawCommands), Commands};

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
-spec get_count_and_data(redis_command()) ->
          {non_neg_integer(), [iolist()]} | {single, binary}.
%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
get_count_and_data({redis_command, Count, Data}) ->
    {Count, Data}.

%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-spec check_result(ok_result() | any()) ->
          normal |
          try_again |
          {moved | ask, redis_lib:addr()}.
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
    {redis_command, 2, [ <<"ASKING\r\n">>, Command]};

add_asking({ok, OldReplies}, {redis_command, _N, Commands}) ->
    %% Extract only the commands with ASK redirection. Even if all commands go to
    %% the same slot it does not have to be the same key. When using hash tags
    %% for instance.
    AskCommands = add_asking_pipeline(OldReplies, Commands),
    %% ASK commands + ASKING
    N = length(AskCommands) * 2,
    {redis_command, N, AskCommands}.

add_asking_pipeline([], _) ->
    [];
add_asking_pipeline([{error, <<"ASK ", _/binary>>} |Replies], [Command |Commands]) ->
    [[ <<"ASKING\r\n">>, Command] | add_asking_pipeline(Replies, Commands)];
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

