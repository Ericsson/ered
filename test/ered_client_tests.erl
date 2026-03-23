-module(ered_client_tests).

-include_lib("eunit/include/eunit.hrl").
-include("ered_test_utils.hrl").

run_test_() ->
    [
     {spawn, fun request_t/0},
     {spawn, fun fail_connect_t/0},
     {spawn, fun fail_parse_t/0},
     {spawn, fun server_close_socket_t/0},
     {spawn, fun bad_request_t/0},
     {spawn, fun server_buffer_full_t/0},
     {spawn, fun low_high_watermark_t/0},
     {spawn, fun bad_option_t/0},
     {spawn, fun bad_connection_option_t/0},
     {spawn, fun server_buffer_full_reconnect_t/0},
     {spawn, fun server_buffer_full_node_goes_down_t/0},
     {spawn, fun response_timeout_t/0},
     {spawn, fun send_backoff_tcp_t/0},
     {spawn, fun send_backoff_tls_t/0},
     {spawn, fun fail_hello_t/0},
     {spawn, fun hello_with_auth_t/0},
     {spawn, fun hello_with_auth_fail_t/0},
     {spawn, fun auth_t/0},
     {spawn, fun auth_fail_t/0},
     {spawn, fun empty_string_host_t/0},
     {spawn, fun bad_host_t/0}
    ].

request_t() ->
    {ok, ListenSock} = gen_tcp:listen(0, [binary, {active , false}]),
    {ok, Port} = inet:port(ListenSock),
    spawn_link(fun() ->
                       {ok, Sock} = gen_tcp:accept(ListenSock),
                       {ok, <<"*1\r\n$4\r\nping\r\n">>} = gen_tcp:recv(Sock, 0),
                       ok = gen_tcp:send(Sock, <<"+pong\r\n">>),
                       receive ok -> ok end
               end),
    Client = start_client(Port),
    {ok, <<"pong">>} = ered_client:command(Client, [<<"ping">>]).


fail_connect_t() ->
    {ok,Pid} = ered_client:start_link("127.0.0.1", 0, [{info_pid, self()}]),
    {connect_error, Reason} = expect_connection_down(Pid),
    true = Reason =:= econnrefused orelse Reason =:= eaddrnotavail,
    %% make sure there are no more connection down messages
    timeout = receive M -> M after 500 -> timeout end.


fail_parse_t() ->
    {ok, ListenSock} = gen_tcp:listen(0, [binary, {active , false}]),
    {ok, Port} = inet:port(ListenSock),
    spawn_link(fun() ->
                       {ok, Sock} = gen_tcp:accept(ListenSock),
                       {ok, <<"*1\r\n$4\r\nping\r\n">>} = gen_tcp:recv(Sock, 0),
                       %% bad format of message
                       ok = gen_tcp:send(Sock, <<"&pong\r\n">>),

                       %% resend from client
                       {ok, Sock2} = gen_tcp:accept(ListenSock),
                       {ok, <<"*1\r\n$4\r\nping\r\n">>} = gen_tcp:recv(Sock2, 0),
                       ok = gen_tcp:send(Sock2, <<"+pong\r\n">>),
                       receive ok -> ok end
               end),
    Client = start_client(Port),
    Pid = self(),
    spawn_link(fun() ->
                       Pid ! ered_client:command(Client, [<<"ping">>])
               end),
    expect_connection_up(Client),
    Reason = {parse_error, {invalid_data, <<"&pong">>}},
    receive #{msg_type := socket_closed, reason := Reason} -> ok end,
    expect_connection_up(Client),
    {ok, <<"pong">>} = get_msg().


server_close_socket_t() ->
    {ok, ListenSock} = gen_tcp:listen(0, [binary, {active , false}]),
    {ok, Port} = inet:port(ListenSock),
    ServerPid =
        spawn_link(fun() ->
                           {ok, Sock} = gen_tcp:accept(ListenSock),
                           receive continue -> ok end,
                           gen_tcp:close(Sock),

                           %% resend from client
                           {ok, _Sock2} = gen_tcp:accept(ListenSock),
                           receive done -> ok end
                   end),
    Client = start_client(Port),
    expect_connection_up(Client),
    ServerPid ! continue,
    receive #{msg_type := socket_closed, reason := tcp_closed} -> ok end,
    expect_connection_up(Client),
    ServerPid ! done.


%% Suppress warning from command 'bad_request'
-dialyzer({no_fail_call, bad_request_t/0}).
bad_request_t() ->
    {ok, ListenSock} = gen_tcp:listen(0, [binary, {active , false}]),
    {ok, Port} = inet:port(ListenSock),
    spawn_link(fun() ->
                       {ok, _Sock} = gen_tcp:accept(ListenSock),
                       receive ok -> ok end
               end),
    Client = start_client(Port),
    expect_connection_up(Client),
    ?_assertException(error, badarg, ered_client:command(Client, bad_request)).


server_buffer_full_t() ->
    {ok, ListenSock} = gen_tcp:listen(0, [binary, {active , false}]),
    {ok, Port} = inet:port(ListenSock),
    spawn_link(fun() ->
                       {ok, Sock} = gen_tcp:accept(ListenSock),
                       %% expect 5 ping
                       Ping = <<"*1\r\n$4\r\nping\r\n">>,
                       Expected = iolist_to_binary(lists:duplicate(5, Ping)),
                       {ok, Expected} = gen_tcp:recv(Sock, size(Expected)),
                       %% should be nothing more since only 5 pending
                       ?assertEqual({error, timeout}, gen_tcp:recv(Sock, 0, 0)),

                       timer:sleep(500),

                       gen_tcp:send(Sock, lists:duplicate(5, <<"+pong\r\n">>)),

                       %% next the 5 waiting
                       {ok, Expected} = gen_tcp:recv(Sock, size(Expected)),
                       %% should be nothing more since only 5 pending
                       {error, timeout} = gen_tcp:recv(Sock, 0, 0),
                       gen_tcp:send(Sock, lists:duplicate(5, <<"+pong\r\n">>)),

                       receive ok -> ok end
               end),
    Client = start_client(Port, [{max_waiting, 5}, {max_pending, 5}, {queue_ok_level,1}]),
    expect_connection_up(Client),

    Pid = self(),
    [ered_client:command_async(Client, [<<"ping">>], fun(Reply) -> Pid ! {N, Reply} end) || N <- lists:seq(1,11)],
    receive #{msg_type := queue_full} -> ok end,
    ?assertMatch({6, {error, queue_overflow}}, get_msg()),
    receive #{msg_type := queue_ok} -> ok end,
    [?assertMatch({N, {ok, <<"pong">>}}, get_msg()) || N <- [1,2,3,4,5,7,8,9,10,11]],
    no_more_msgs().

low_high_watermark_t() ->
    {ok, ListenSock} = gen_tcp:listen(0, [binary, {active, false}]),
    {ok, Port} = inet:port(ListenSock),
    ServerPid = spawn_link(fun() ->
                                   {ok, Sock} = gen_tcp:accept(ListenSock),
                                   Ping = <<"*1\r\n$4\r\nping\r\n">>,
                                   FivePing = iolist_to_binary(lists:duplicate(5, Ping)),
                                   {ok, FivePing} = gen_tcp:recv(Sock, size(FivePing)),

                                   %% should be nothing more since only 5 pending
                                   ?assertEqual({error, timeout}, gen_tcp:recv(Sock, 0, 0)),

                                   gen_tcp:send(Sock, lists:duplicate(4, <<"+pong\r\n">>)),
                                   receive send_one_more_pong -> ok end,
                                   gen_tcp:send(Sock, lists:duplicate(1, <<"+pong\r\n">>)),

                                   {ok, FivePing} = gen_tcp:recv(Sock, 0),
                                   gen_tcp:send(Sock, lists:duplicate(5, <<"+pong\r\n">>)),
                                   receive ok -> ok end
                           end),
    Client = start_client(Port, [{connection_opts, [{batch_size,5}]}, {max_waiting, 10}, {max_pending, 5}, {queue_ok_level,1}]),
    expect_connection_up(Client),

    Pid = self(),
    [ered_client:command_async(Client, [<<"ping">>], fun(Reply) -> Pid ! {N, Reply} end) || N <- lists:seq(1,10)],
    [?assertEqual({N, {ok, <<"pong">>}}, get_msg()) || N <- [1,2,3,4]],

    %% high water mark is hit, and we can not fill more until we reach low water-mark.
    ?assertMatch(#{pending := {1, _},
                   waiting := {5, _},
                   filling_batch := false},
                 ered_client:state_to_map(sys:get_state(Client))),

    ServerPid ! send_one_more_pong,
    [?assertEqual({N, {ok, <<"pong">>}}, get_msg()) || N <- [5]],

    %% low water mark reached, pending should now be filled.
    ?assertMatch(#{pending := {5, _},
                   waiting := {0, _},
                   filling_batch := false},
                 ered_client:state_to_map(sys:get_state(Client))),

    [?assertEqual({N, {ok, <<"pong">>}}, get_msg()) || N <- [6,7,8,9,10]],

    ?assertMatch(#{pending := {0, _},
                   waiting := {0, _},
                   filling_batch := true},
                 ered_client:state_to_map(sys:get_state(Client))),

    no_more_msgs().


server_buffer_full_reconnect_t() ->
    {ok, ListenSock} = gen_tcp:listen(0, [binary, {active , false}]),
    {ok, Port} = inet:port(ListenSock),
    spawn_link(fun() ->
                       {ok, Sock} = gen_tcp:accept(ListenSock),
                       Ping = <<"*1\r\n$4\r\nping\r\n">>,
                       FivePing = iolist_to_binary(lists:duplicate(5, Ping)),
                       {ok, FivePing} = gen_tcp:recv(Sock, size(FivePing)),
                       %% should be nothing more since only 5 pending
                       {error, timeout} = gen_tcp:recv(Sock, 0, 0),

                       gen_tcp:close(Sock),

                       {ok, Sock2} = gen_tcp:accept(ListenSock),
                       {ok, FivePing} = gen_tcp:recv(Sock2, size(FivePing)),

                       gen_tcp:send(Sock2, lists:duplicate(5, <<"+pong\r\n">>)),
                       %% should be nothing more since only 5 pending
                       {error, timeout} = gen_tcp:recv(Sock2, 0, 0),
                       receive ok -> ok end

               end),
    Client = start_client(Port, [{max_waiting, 5}, {max_pending, 5}, {queue_ok_level,1}]),
    expect_connection_up(Client),

    Pid = self(),
    %% 5 messages will be pending, 5 messages in queue
    [ered_client:command_async(Client, [<<"ping">>], fun(Reply) -> Pid ! {N, Reply} end) || N <- lists:seq(1,11)],
    receive #{msg_type := queue_full} -> ok end,
    %% 1 message over the limit, first one in queue gets kicked out
    {6, {error, queue_overflow}} = get_msg(),
    receive #{msg_type := socket_closed, reason := tcp_closed} -> ok end,
    %% when connection goes down the pending messages will be put in the queue and the queue
    %% will overflow kicking out the oldest first
    [{N, {error, queue_overflow}} = get_msg() || N <- [1,2,3,4,5]],
    receive #{msg_type := queue_ok} -> ok end,
    expect_connection_up(Client),
    [{N, {ok, <<"pong">>}} = get_msg() || N <- [7,8,9,10,11]],
    no_more_msgs().


server_buffer_full_node_goes_down_t() ->
    {ok, ListenSock} = gen_tcp:listen(0, [binary, {active , false}]),
    {ok, Port} = inet:port(ListenSock),
    spawn_link(fun() ->
                       {ok, Sock} = gen_tcp:accept(ListenSock),
                       Ping = <<"*1\r\n$4\r\nping\r\n">>,
                       FivePing = iolist_to_binary(lists:duplicate(5, Ping)),
                       {ok, FivePing} = gen_tcp:recv(Sock, size(FivePing)),
                       %% should be nothing more since only 5 pending
                       {error, timeout} = gen_tcp:recv(Sock, 0, 0),
                       gen_tcp:close(ListenSock)
               end),
    Client = start_client(Port, [{max_waiting, 5}, {max_pending, 5}, {queue_ok_level,1}, {node_down_timeout, 100}]),
    expect_connection_up(Client),

    Pid = self(),
    [ered_client:command_async(Client, [<<"ping">>], fun(Reply) -> Pid ! {N, Reply} end) || N <- lists:seq(1,11)],
    receive #{msg_type := queue_full} -> ok end,
    ?assertEqual({6, {error, queue_overflow}}, get_msg()),
    receive #{msg_type := socket_closed, reason := tcp_closed} -> ok end,
    [?assertEqual({N, {error, queue_overflow}}, get_msg()) || N <- [1,2,3,4,5]],
    receive #{msg_type := queue_ok} -> ok end,
    receive #{msg_type := connect_error, reason := econnrefused} -> ok end,
    receive #{msg_type := node_down_timeout} -> ok end,
    [{N, {error, node_down}} = get_msg() || N <- [7,8,9,10,11]],

    %% additional commands should get a node down
    {error, node_down} =  ered_client:command(Client, [<<"ping">>]),
    no_more_msgs().


%% Suppress warning from option 'bad_option'
-dialyzer({no_fail_call, bad_option_t/0}).
bad_option_t() ->
    ?_assertError({badarg,bad_option}, ered_client:start_link("127.0.0.1", 0, [bad_option])).

bad_connection_option_t() ->
    ?_assertError({badarg,bad_option}, ered_client:start_link("127.0.0.1", 0,
                                                              [{info_pid, self()},
                                                               {connection_opts, [bad_option]}])).

response_timeout_t() ->
    {ok, ListenSock} = gen_tcp:listen(0, [binary, {active , false}]),
    {ok, Port} = inet:port(ListenSock),
    spawn_link(fun() ->
                       {ok, Sock} = gen_tcp:accept(ListenSock),
                       {ok, <<"*1\r\n$4\r\nping\r\n">>} = gen_tcp:recv(Sock, 0),

                       %% do nothing more on first socket, wait for timeout and reconnect
                       {ok, Sock2} = gen_tcp:accept(ListenSock),
                       {ok, <<"*1\r\n$4\r\nping\r\n">>} = gen_tcp:recv(Sock2, 0),
                       ok = gen_tcp:send(Sock2, <<"+pong\r\n">>),
                       receive ok -> ok end
               end),
    Client = start_client(Port, [{connection_opts, [{response_timeout, 100}]}]),
    expect_connection_up(Client),
    Pid = self(),
    ered_client:command_async(Client, [<<"ping">>], fun(Reply) -> Pid ! {reply, Reply} end),
    %% this should come after max 1000ms
    receive #{msg_type := socket_closed, reason := timeout} -> ok after 2000 -> timeout_error() end,
    expect_connection_up(Client),
    {reply, {ok, <<"pong">>}} = get_msg(),
    no_more_msgs().

send_backoff_tcp_t() ->
    send_backoff_t(gen_tcp).

send_backoff_tls_t() ->
    %% ssl with {send_timeout, 0} closes the socket on timeout in
    %% OTP 28.0 .. 28.4 (ssl 11.2.0 .. 11.5.2). It was intentionally
    %% broken and fixed in ssl-11.5.3 (OTP-20018).
    %% It works in OTP 27 and earlier.
    application:load(ssl),
    {ok, SslVsn} = application:get_key(ssl, vsn),
    case vsn_ge(SslVsn, "11.2.0") andalso not vsn_ge(SslVsn, "11.5.3") of
        true -> ok; %% skip
        false -> send_backoff_t(ssl)
    end.

send_backoff_t(Transport) ->
    %% Send a large command N times.
    N = 10,

    %% Construct a large binary.
    Size = 1000 * 1000,
    LargeBinary = binary:copy(<<"a">>, Size),
    LargeCommand = [<<"SET">>, <<"foo">>, LargeBinary],
    Resp = ered_command:get_data(
             ered_command:convert_to([<<"SET">>, <<"foo">>, LargeBinary])),
    RespLen = byte_size(Resp),

    %% Start server
    {ListenSock, Port, ConnOpts} = listen(Transport),
    ServerPid =
        spawn_link(fun() ->
                           Sock = accept(Transport, ListenSock),
                           receive continue -> ok end,
                           [begin
                                {ok, <<"*3\r\n$3\r\nSET\r", _/binary>>} = Transport:recv(Sock, RespLen),
                                ok = Transport:send(Sock, <<"+OK\r\n">>)
                            end || _ <- lists:seq(1, N)],
                           {ok, <<"*1\r\n$4\r\nping\r\n">>} = Transport:recv(Sock, 0),
                           ok = Transport:send(Sock, <<"+PONG\r\n">>),
                           receive ok -> ok end
                   end),
    Client = start_client(Port, [{connection_opts, ConnOpts ++ [{batch_size, 1}]}]),
    expect_connection_up(Client),
    Pid = self(),
    %% Send the large command N times. In some cases, gen_tcp:send returns
    %% {error, timeout} after a few times, even if the data is really large.
    [ered_client:command_async(Client, LargeCommand, fun(Reply) -> Pid ! {reply, Reply} end)
     || _ <- lists:seq(1, N)],

    #{backoff_send := BackoffSend,
      pending := {NumPending, _},
      waiting := {NumWaiting, _}} = ered_client:state_to_map(sys:get_state(Client)),
    ?assert(BackoffSend),
    ?assert(NumPending > 0),
    ?assert(NumWaiting > 0),
    ?assertEqual(N, NumWaiting + NumPending),
    ServerPid ! continue,
    [{reply, {ok, <<"OK">>}} = get_msg() || _ <- lists:seq(1, N)],
    ered_client:command_async(Client, [<<"ping">>], fun(Reply) -> Pid ! {reply, Reply} end),
    {reply, {ok, <<"PONG">>}} = get_msg(),
    no_more_msgs().

fail_hello_t() ->
    {ok, ListenSock} = gen_tcp:listen(0, [binary, {active , false}]),
    {ok, Port} = inet:port(ListenSock),
    Pid = self(),
    spawn_link(fun() ->
                       {ok, Sock} = gen_tcp:accept(ListenSock),
                       {ok, <<"*2\r\n$5\r\nHELLO\r\n$1\r\n3\r\n">>} = gen_tcp:recv(Sock, 0),
                       ok = gen_tcp:send(Sock, <<"-NOPROTO unsupported protocol version\r\n">>),

                       Pid ! done,
                       {error, closed} = gen_tcp:recv(Sock, 0)
               end),
    {ok,Client} = ered_client:start_link("127.0.0.1", Port, [{info_pid, self()}]),
    receive done -> ok end,
    {init_error, [<<"NOPROTO unsupported protocol version">>]} = expect_connection_down(Client),
    no_more_msgs().

hello_with_auth_t() ->
    {ok, ListenSock} = gen_tcp:listen(0, [binary, {active , false}]),
    {ok, Port} = inet:port(ListenSock),
    Pid = self(),
    spawn_link(fun() ->
                       {ok, Sock} = gen_tcp:accept(ListenSock),
                       {ok, <<"*5\r\n"
                              "$5\r\nHELLO\r\n"
                              "$1\r\n3\r\n"
                              "$4\r\nAUTH\r\n"
                              "$3\r\nali\r\n"
                              "$6\r\nsesame\r\n">>} = gen_tcp:recv(Sock, 0),
                       ok = gen_tcp:send(Sock, <<"%7\r\n"
                                                 "$6\r\nserver\r\n"
                                                 "$5\r\nredis\r\n"
                                                 "$7\r\nversion\r\n"
                                                 "$5\r\n6.0.0\r\n"
                                                 "$5\r\nproto\r\n"
                                                 ":3\r\n"
                                                 "$2\r\nid\r\n"
                                                 ":10\r\n"
                                                 "$4\r\nmode\r\n"
                                                 "$10\r\nstandalone\r\n"
                                                 "$4\r\nrole\r\n"
                                                 "$6\r\nmaster\r\n"
                                                 "$7\r\nmodules\r\n"
                                                 "*0\r\n">>),
                       Pid ! done,
                       receive ok -> ok end
               end),
    {ok, Client} = ered_client:start_link("127.0.0.1", Port, [{info_pid, self()},
                                                              {auth, {<<"ali">>, <<"sesame">>}}]),
    receive done -> ok end,
    expect_connection_up(Client),
    no_more_msgs().

hello_with_auth_fail_t() ->
    {ok, ListenSock} = gen_tcp:listen(0, [binary, {active , false}]),
    {ok, Port} = inet:port(ListenSock),
    spawn_link(fun() ->
                       {ok, Sock} = gen_tcp:accept(ListenSock),
                       {ok, <<"*5\r\n"
                              "$5\r\nHELLO\r\n"
                              "$1\r\n3\r\n"
                              "$4\r\nAUTH\r\n"
                              "$3\r\nali\r\n"
                              "$6\r\nsesame\r\n">>} = gen_tcp:recv(Sock, 0),
                       ok = gen_tcp:send(Sock,
                                         <<"-WRONGPASS invalid username-password"
                                           " pair or user is disabled.\r\n">>)
               end),
    {ok, Client} = ered_client:start_link("127.0.0.1", Port, [{info_pid, self()},
                                                              {auth, {<<"ali">>, <<"sesame">>}}]),
    {init_error, [<<"WRONGPASS", _/binary>>]} = expect_connection_down(Client),
    no_more_msgs().

auth_t() ->
    {ok, ListenSock} = gen_tcp:listen(0, [binary, {active , false}]),
    {ok, Port} = inet:port(ListenSock),
    Pid = self(),
    spawn_link(fun() ->
                       {ok, Sock} = gen_tcp:accept(ListenSock),
                       {ok, <<"*3\r\n"
                              "$4\r\nAUTH\r\n"
                              "$3\r\nali\r\n"
                              "$6\r\nsesame\r\n">>} = gen_tcp:recv(Sock, 0),
                       ok = gen_tcp:send(Sock, <<"+OK\r\n">>),

                       Pid ! done,
                       receive ok -> ok end
               end),
    {ok, Client} = ered_client:start_link("127.0.0.1", Port, [{info_pid, self()},
                                                              {resp_version, 2},
                                                              {auth, {<<"ali">>, <<"sesame">>}}]),
    receive done -> ok end,
    expect_connection_up(Client),
    no_more_msgs().

auth_fail_t() ->
    {ok, ListenSock} = gen_tcp:listen(0, [binary, {active , false}]),
    {ok, Port} = inet:port(ListenSock),
    spawn_link(fun() ->
                       {ok, Sock} = gen_tcp:accept(ListenSock),
                       {ok, <<"*3\r\n"
                              "$4\r\nAUTH\r\n"
                              "$3\r\nali\r\n"
                              "$6\r\nsesame\r\n">>} = gen_tcp:recv(Sock, 0),
                       ok = gen_tcp:send(Sock,
                                         <<"-WRONGPASS invalid username-password"
                                           " pair or user is disabled.\r\n">>)
               end),
    {ok,Client} = ered_client:start_link("127.0.0.1", Port, [{info_pid, self()},
                                                             {resp_version, 2},
                                                             {auth, {<<"ali">>, <<"sesame">>}}]),
    ?MSG(#{msg_type := init_error, reason := [<<"WRONGPASS", _/binary>>], client_id := Client},
         10000).

-if(?OTP_RELEASE >= 26).
%% OTP >= 26 uses an empty hostname in the address lookup, which fails. See OTP-18543.
-define(empty_string_host_reason, nxdomain).
-else.
%% OTP < 26 sees an empty hostname as a bad argument.
-define(empty_string_host_reason, {'EXIT',badarg}).
-endif.

empty_string_host_t() ->
    {ok,Client} = ered_client:start_link("", 30000, [{info_pid, self()}]),
    ?MSG(#{msg_type := connect_error, reason := ?empty_string_host_reason, client_id := Client},
         20000).

bad_host_t() ->
    {ok,Client} = ered_client:start_link(undefined, 30000, [{info_pid, self()}]),
    ?MSG(#{msg_type := connect_error, reason := nxdomain, client_id := Client},
         20000).

expect_connection_up(Client) ->
    expect_connection_up(Client, infinity).

expect_connection_up(Client, Timeout) ->
    #{msg_type := connected, client_id := Client} =
        get_msg(Timeout).

expect_connection_down(Client) ->
    expect_connection_down(Client, infinity).

expect_connection_down(Client, Timeout) ->
    #{msg_type := MsgType, reason := Reason, client_id := Client} =
        get_msg(Timeout),
    if Reason =/= none -> ok end,
    {MsgType, Reason}.

get_msg() ->
    get_msg(infinity).

get_msg(Timeout) ->
    receive Msg -> Msg after Timeout -> timeout end.

no_more_msgs() ->
    timeout = get_msg(0).


start_client(Port) ->
    start_client(Port, []).

start_client(Port, Opt) ->
    {ok, Client} = ered_client:start_link("127.0.0.1", Port, [{info_pid, self()}, {resp_version,2}] ++ Opt),
    Client.

timeout_error() ->
    error({timeout, erlang:process_info(self(), messages)}).

listen(gen_tcp) ->
    {ok, LSock} = gen_tcp:listen(0, [binary, {active, false}]),
    {ok, Port} = inet:port(LSock),
    {LSock, Port, [{tcp_options, []}]};
listen(ssl) ->
    ssl:start(),
    CertFile = "/tmp/ered_test_cert.pem",
    KeyFile = "/tmp/ered_test_key.pem",
    os:cmd("openssl req -x509 -newkey rsa:2048 -keyout " ++ KeyFile ++
               " -out " ++ CertFile ++ " -days 1 -nodes -subj '/CN=localhost' 2>/dev/null"),
    {ok, LSock} = ssl:listen(0, [binary, {active, false},
                                 {certfile, CertFile}, {keyfile, KeyFile}]),
    {ok, {_, Port}} = ssl:sockname(LSock),
    {LSock, Port, [{tls_options, [{verify, verify_none}]}]}.

accept(gen_tcp, LSock) ->
    {ok, Sock} = gen_tcp:accept(LSock),
    Sock;
accept(ssl, LSock) ->
    {ok, TSock} = ssl:transport_accept(LSock),
    {ok, Sock} = ssl:handshake(TSock),
    Sock.


vsn_ge(Vsn1, Vsn2) ->
    lists:map(fun list_to_integer/1, string:tokens(Vsn1, ".")) >=
        lists:map(fun list_to_integer/1, string:tokens(Vsn2, ".")).
