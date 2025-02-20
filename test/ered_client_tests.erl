-module(ered_client_tests).

-include_lib("eunit/include/eunit.hrl").

run_test_() ->
    [
     {spawn, fun request_t/0},
     {spawn, fun fail_connect_t/0},
     {spawn, fun fail_parse_t/0},
     {spawn, fun server_close_socket_t/0},
     {spawn, fun bad_request_t/0},
     {spawn, fun server_buffer_full_t/0},
     {spawn, fun bad_option_t/0},
     {spawn, fun bad_connection_option_t/0},
     {spawn, fun server_buffer_full_reconnect_t/0},
     {spawn, fun server_buffer_full_node_goes_down_t/0},
     {spawn, fun send_timeout_t/0},
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
    {connect_error,econnrefused} = expect_connection_down(Pid),
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
    Reason = {recv_exit, {parse_error,{invalid_data,<<"&pong">>}}},
    receive #{msg_type := socket_closed, reason := Reason} -> ok end,
    expect_connection_up(Client),
    {ok, <<"pong">>} = get_msg().


server_close_socket_t() ->
    {ok, ListenSock} = gen_tcp:listen(0, [binary, {active , false}]),
    {ok, Port} = inet:port(ListenSock),
    spawn_link(fun() ->
                       {ok, Sock} = gen_tcp:accept(ListenSock),
                       gen_tcp:close(Sock),

                       %% resend from client
                       {ok, _Sock2} = gen_tcp:accept(ListenSock),
                       receive ok -> ok end
               end),
    Client = start_client(Port),
    expect_connection_up(Client),
    receive #{msg_type := socket_closed, reason := {recv_exit, closed}} -> ok end,
    expect_connection_up(Client).


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
                       {error, timeout} = gen_tcp:recv(Sock, 0, 0),

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
    {6, {error, queue_overflow}} = get_msg(),
    receive #{msg_type := queue_ok} -> ok end,
    [{N, {ok, <<"pong">>}} = get_msg()|| N <- [1,2,3,4,5,7,8,9,10,11]],
    no_more_msgs().



server_buffer_full_reconnect_t() ->
    {ok, ListenSock} = gen_tcp:listen(0, [binary, {active , false}]),
    {ok, Port} = inet:port(ListenSock),
    spawn_link(fun() ->
                       {ok, Sock} = gen_tcp:accept(ListenSock),
                       %% expect 5 ping
                       Ping = <<"*1\r\n$4\r\nping\r\n">>,
                       Expected = iolist_to_binary(lists:duplicate(5, Ping)),
                       {ok, Expected} = gen_tcp:recv(Sock, size(Expected)),
                       %% should be nothing more since only 5 pending
                       {error, timeout} = gen_tcp:recv(Sock, 0, 0),

                       gen_tcp:close(Sock),

                       {ok, Sock2} = gen_tcp:accept(ListenSock),
                       {ok, Expected} = gen_tcp:recv(Sock2, size(Expected)),

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
    receive #{msg_type := socket_closed, reason := {recv_exit, closed}} -> ok end,
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
                       %% expect 5 ping
                       Ping = <<"*1\r\n$4\r\nping\r\n">>,
                       Expected = iolist_to_binary(lists:duplicate(5, Ping)),
                       {ok, Expected} = gen_tcp:recv(Sock, size(Expected)),
                       %% should be nothing more since only 5 pending
                       {error, timeout} = gen_tcp:recv(Sock, 0, 0),
                       gen_tcp:close(ListenSock)
               end),
    Client = start_client(Port, [{max_waiting, 5}, {max_pending, 5}, {queue_ok_level,1}, {node_down_timeout, 100}]),
    expect_connection_up(Client),

    Pid = self(),
    [ered_client:command_async(Client, [<<"ping">>], fun(Reply) -> Pid ! {N, Reply} end) || N <- lists:seq(1,11)],
    receive #{msg_type := queue_full} -> ok end,
    {6, {error, queue_overflow}} = get_msg(),
    receive #{msg_type := socket_closed, reason := {recv_exit, closed}} -> ok end,
    [{N, {error, queue_overflow}} = get_msg() || N <- [1,2,3,4,5]],
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

send_timeout_t() ->
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
    receive #{msg_type := socket_closed, reason := {recv_exit, timeout}} -> ok after 2000 -> timeout_error() end,
    expect_connection_up(Client),
    {reply, {ok, <<"pong">>}} = get_msg(),
    no_more_msgs().

fail_hello_t() ->
    {ok, ListenSock} = gen_tcp:listen(0, [binary, {active , false}]),
    {ok, Port} = inet:port(ListenSock),
    Pid = self(),
    spawn_link(fun() ->
                       {ok, Sock} = gen_tcp:accept(ListenSock),
                       {ok, <<"*2\r\n$5\r\nHELLO\r\n$1\r\n3\r\n">>} = gen_tcp:recv(Sock, 0),
                       ok = gen_tcp:send(Sock, <<"-NOPROTO unsupported protocol version\r\n">>),

                       %% test resend
                       {ok, <<"*2\r\n$5\r\nHELLO\r\n$1\r\n3\r\n">>} = gen_tcp:recv(Sock, 0),
                       ok = gen_tcp:send(Sock, <<"-NOPROTO unsupported protocol version\r\n">>),

                       Pid ! done
               end),
    {ok,Client} = ered_client:start_link("127.0.0.1", Port, [{info_pid, self()}]),
    {init_error, [<<"NOPROTO unsupported protocol version">>]} = expect_connection_down(Client),
    receive done -> ok end,
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
                       Pid ! done
               end),
    {ok, _Client} = ered_client:start_link("127.0.0.1", Port, [{info_pid, self()},
                                                               {auth, {<<"ali">>, <<"sesame">>}}]),
    receive done -> ok end,
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

                       Pid ! done
               end),
    {ok, _Client} = ered_client:start_link("127.0.0.1", Port, [{info_pid, self()},
                                                               {resp_version, 2},
                                                               {auth, {<<"ali">>, <<"sesame">>}}]),
    receive done -> ok end,
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
    {init_error, [<<"WRONGPASS", _/binary>>]} = expect_connection_down(Client),
    no_more_msgs().

-if(?OTP_RELEASE >= 26).
%% OTP >= 26 uses an empty hostname in the address lookup, which fails. See OTP-18543.
-define(empty_string_host_reason, nxdomain).
-else.
%% OTP < 26 sees an empty hostname as a bad argument.
-define(empty_string_host_reason, {'EXIT',badarg}).
-endif.

empty_string_host_t() ->
    {ok,Client} = ered_client:start_link("", 30000, [{info_pid, self()}]),
    {connect_error, ?empty_string_host_reason} = expect_connection_down(Client),
    no_more_msgs().

bad_host_t() ->
    {ok,Client} = ered_client:start_link(undefined, 30000, [{info_pid, self()}]),
    {connect_error, nxdomain} = expect_connection_down(Client),
    no_more_msgs().

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

