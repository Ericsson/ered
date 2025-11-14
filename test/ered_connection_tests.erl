-module(ered_connection_tests).

-include_lib("eunit/include/eunit.hrl").

split_data_test() ->
    Data = << <<"A">> || _ <- lists:seq(1, 3000) >>,
    {ok, ListenSock} = gen_tcp:listen(0, [binary, {active , false}]),
    {ok, Port} = inet:port(ListenSock),
    spawn_link(fun() ->
                       {ok, Sock} = gen_tcp:accept(ListenSock),
                       {ok, <<"*2\r\n$5\r\nhello\r\n$1\r\n3\r\n">>} = gen_tcp:recv(Sock, 0),
                       HelloReply = <<"%7\r\n",
                                      "$6\r\nserver\r\n", "$6\r\nvalkey\r\n",
                                      "$7\r\nversion\r\n", "$5\r\n9.0.0\r\n",
                                      "$5\r\nproto\r\n", ":3\r\n"
                                      "$2\r\nid\r\n", ":2\r\n",
                                      "$4\r\nmode\r\n", "$10\r\nstandalone\r\n"
                                      "$4\r\nrole\r\n", "$6\r\nmaster\r\n"
                                      "$7\r\nmodules\r\n", "*0\r\n">>,
                       ok = gen_tcp:send(Sock, HelloReply),
                       SetCommand = <<"*3\r\n$3\r\nset\r\n$4\r\nkey1\r\n$3000\r\n", Data/binary, "\r\n">>,
                       {ok, SetCommand} = gen_tcp:recv(Sock, size(SetCommand)),
                       ok = gen_tcp:send(Sock, <<"+OK\r\n">>),
                       {ok, <<"*2\r\n$3\r\nget\r\n$4\r\nkey1\r\n">>} = gen_tcp:recv(Sock, 0),
                       ok = gen_tcp:send(Sock, <<"$3000\r\n", Data/binary, "\r\n">>),
                       {error, closed} = gen_tcp:recv(Sock, 0),
                       %% ok = gen_tcp:shutdown(Sock, write),
                       ok
               end),
    {ok, Conn1} = ered_connection:connect("127.0.0.1", Port),
    ered_connection:command(Conn1, [<<"hello">>, <<"3">>]),
    <<"OK">> = ered_connection:command(Conn1, [<<"set">>, <<"key1">>, Data]),
    Data = ered_connection:command(Conn1, [<<"get">>, <<"key1">>]).

%% Suppress warnings due to expected failures from MalformedCommand.
-dialyzer({[no_fail_call, no_return], trailing_reply_test/0}).
trailing_reply_test() ->
    Pid = self(),
    %% 277124 byte nested array, it takes a non-trivial time to parse
    BigNastyData = iolist_to_binary(nested_list(8)),
    ?debugFmt("~w", [size(BigNastyData)]),

    spawn_link(fun() ->
                       {ok, ListenSock} = gen_tcp:listen(0, [binary, {active , false}]),
                       {ok, Port} = inet:port(ListenSock),
                       Pid ! {port, Port},
                       {ok, Sock} = gen_tcp:accept(ListenSock),
                       {ok, <<"*1\r\n$4\r\nping\r\n">>} = gen_tcp:recv(Sock, 0),
                       ok = gen_tcp:send(Sock, BigNastyData),
                       ok = gen_tcp:shutdown(Sock, write),
                       Pid ! sent_big_nasty,
                       receive ok -> ok end
               end),
    {port, Port} = receive_msg(),
    %% increase receive buffer to fit the whole nasty data package
    {ok, Conn1} = ered_connection:connect("127.0.0.1", Port, [{batch_size, 1},
                                                              {tcp_options, [{recbuf, 524288}]}]),
    ?debugFmt("~w", [Conn1]),
    ered_connection:command_async(Conn1, [<<"ping">>], ping1),
    receive sent_big_nasty -> ok end,
    MalformedCommand = {redis_command, pipeline, [undefined]},
    ered_connection:command_async(Conn1, MalformedCommand, no_ref),

    %% make sure the ping is received before the connection is shut down

    ?debugMsg("waiting for ping"),

    receive {ping1, _} -> ok after 2000 -> exit(waiting_for_ping) end,
    ?debugMsg("got ping"),
    {socket_closed, Conn1, {send_exit, einval}} = receive Msg -> Msg end,
    ensure_empty().


receive_msg() ->
    receive Msg -> Msg end.

%% This function is used from trailing_reply_test()
-dialyzer({no_unused, ensure_empty/0}).
ensure_empty() ->
    empty = receive Msg -> Msg after 0 -> empty end.


nested_list(1) ->
    <<"+A\r\n">>;
nested_list(N) ->
    ["*", integer_to_list(N), "\r\n", [nested_list(N-1) || _ <- lists:seq(1, N)]].
