-module(redis_command).

-export([convert_to/1,
         add_asking/2,
         check_result/1,
         fix_ask_reply/2]).

-type command() :: {redis_command,
                    non_neg_integer() | single,
                    binary() | [binary()]}.

-export_type([command/0]).

-spec convert_to(binary() | iolist() | command()) -> command().

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


add_asking(_, {redis_command, single, Command}) ->
    {redis_command, 2, [ <<"ASKING\r\n">>, Command]};

add_asking({ok, OldReplies}, {redis_command, _N, Commands}) ->
    %% Extract only the commands with ASK redirection
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

