-module(redis_connection_tests).

-include_lib("eunit/include/eunit.hrl").

-compile([export_all]).


%% TODO this requires a running redis instance, move to common test

%% push_test() ->
%%     Pid = self(),
%%     CB = fun(V) -> Pid ! V end,
%%     Conn1 = redis_connection:connect("127.0.0.1", 6379, [{push_cb, CB}]),
%%     Result1 = redis_connection:request(Conn1, [<<"hello">>, <<"3">>]),
%%     3 = maps:get(<<"proto">>, Result1),
%%     [<<"subscribe">>,<<"first">>,1] = redis_connection:request(Conn1, [<<"subscribe">>, <<"first">>]),
%%     <<"PONG">> = redis_connection:request(Conn1, [<<"ping">>]),

%%     Conn2 = redis_connection:connect("127.0.0.1", 6379),
%%     Result2 = redis_connection:request(Conn2, [<<"publish">>, <<"first">>, <<"hi">>]),
%%     true = is_integer(Result2),

%%     [<<"message">>, <<"first">>, <<"hi">>] = receive Msg -> Msg after 1000 -> timeout end,
%%     <<"PONG">> = redis_connection:request(Conn1, [<<"ping">>]).


split_data_test() ->
    Data = iolist_to_binary([<<"A">> || _ <- lists:seq(0,3000)]),
    {ok, Conn1} = redis_connection:connect("127.0.0.1", 6379),
    redis_connection:request(Conn1, [<<"hello">>, <<"3">>]),
    <<"OK">> = redis_connection:request(Conn1, [<<"set">>, <<"key1">>, Data]),
    Data = redis_connection:request(Conn1, [<<"get">>, <<"key1">>]).

trailing_reply_test() ->
    Pid = self(),
    % 277124 byte nested array, it takes a non-trivial time to parse
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
    {ok, Conn1} = redis_connection:connect("127.0.0.1", Port, [{batch_size, 1},
                                                               {tcp_options, [{recbuf, 524288}]}]),
    ?debugFmt("~w", [Conn1]),
    redis_connection:request_async(Conn1, [<<"ping">>], ping1),
%    process_flag(trap_exit, true),
    receive sent_big_nasty -> ok end,
    redis_connection:request_async_raw(Conn1, undefined, apa),
    %% make sure the ping is received before the connection is shut down

    ?debugMsg("waiting for ping"),

    receive {ping1, _} -> ok after 2000 -> exit(waiting_for_ping) end,
    ?debugMsg("got ping"),
%    {'EXIT', Conn1, {send_error, einval}} = receive Msg -> Msg end,
    {socket_closed, Conn1, {send_exit, einval}} = receive Msg -> Msg end,
    ensure_empty().


receive_msg() ->
    receive Msg -> Msg end.

ensure_empty() ->
    empty = receive Msg -> Msg after 0 -> empty end.


nested_list(1) ->
    <<"+A\r\n">>;
nested_list(N) ->
   ["*", integer_to_list(N), "\r\n", [nested_list(N-1) || _ <- lists:seq(1, N)]].


nested_list1(1) ->
    <<"A">>;
nested_list1(N) ->
    [nested_list1(N-1) || _ <- lists:seq(1, N)].
