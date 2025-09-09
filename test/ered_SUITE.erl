-module(ered_SUITE).

-include("ered_test_utils.hrl").

-compile([export_all, nowarn_export_all]).

all() ->
    [
     t_command,
     t_command_async,
     t_command_all,
     t_command_client,
     t_command_pipeline,
     t_cluster_crash,
     t_client_crash,
     t_client_killed,
     t_scan_delete_keys,
     t_hard_failover,
     t_manual_failover,
     t_manual_failover_then_old_master_down,
     t_blackhole,
     t_blackhole_all_nodes,
     t_connect_timeout,
     t_init_timeout,
     t_empty_slotmap,
     t_empty_initial_slotmap,
     t_split_data,
     t_subscribe,
     t_queue_full,
     t_kill_client,
     t_new_cluster_master,
     t_ask_redirect,
     t_missing_slot,
     t_client_map
    ].

-define(PORTS, [30001, 30002, 30003, 30004, 30005, 30006]).

-define(DEFAULT_SERVER_DOCKER_IMAGE, "valkey/valkey:8.0.1").

-define(CLIENT_OPTS, [{connection_opts, [{connect_timeout, 500}]}]).

init_per_suite(_Config) ->
    stop_containers(), % just in case there is junk from previous runs
    Image = os:getenv("SERVER_DOCKER_IMAGE", ?DEFAULT_SERVER_DOCKER_IMAGE),
    EnableDebugCommand = case Image of
                             "redis:" ++ [N, $. | _] when N >= $1, N < $7 ->
                                 ""; % Option does not exist.
                             _Redis7 ->
                                 " --enable-debug-command yes"
                         end,
    cmd_log([io_lib:format("docker run --name redis-~p -d --net=host"
                           " --restart=on-failure ~s redis-server"
                           "~s"
                           " --cluster-enabled yes --port ~p"
                           " --cluster-node-timeout 2000;",
                           [P, Image, EnableDebugCommand, P])
             || P <- ?PORTS]),

    {ok, _} = application:ensure_all_started(ered, temporary),
    ered_test_utils:wait_for_all_nodes_available(?PORTS, ?CLIENT_OPTS),
    create_cluster(),
    wait_for_consistent_cluster(),
    [].

init_per_testcase(_Testcase, Config) ->
    %% Quick check that cluster is OK; otherwise restart everything.
    case catch ered_test_utils:check_consistent_cluster(?PORTS, ?CLIENT_OPTS) of
        ok ->
            [];
        _ ->
            ct:pal("Re-initialize the cluster"),
            init_per_suite(Config)
    end.

create_cluster() ->
    Image = os:getenv("SERVER_DOCKER_IMAGE", ?DEFAULT_SERVER_DOCKER_IMAGE),
    Hosts = [io_lib:format("127.0.0.1:~p ", [P]) || P <- ?PORTS],
    Cmd = io_lib:format("echo 'yes' | "
                        "docker run --name redis-cluster --rm --net=host -i ~s "
                        "redis-cli --cluster-replicas 1 --cluster create ~s",
                        [Image, Hosts]),
    cmd_log(Cmd).

reset_cluster() ->
    Pids = [begin
                {ok, Pid} = ered_client:connect("127.0.0.1", Port, []),
                Pid
            end || Port <- ?PORTS],
    [{ok, <<"OK">>} = ered_client:command(Pid, [<<"CLUSTER">>, <<"RESET">>]) || Pid <- Pids],
    [{ok, []} = ered_client:command(Pid, [<<"CLUSTER">>, <<"SLOTS">>]) || Pid <- Pids],
    lists:foreach(fun ered_client:close/1, Pids).

%% Wait until cluster is consistent, i.e all nodes have the same single view
%% of the slot map and all cluster nodes are included in the slot map.
wait_for_consistent_cluster() ->
    wait_for_consistent_cluster(?PORTS).

wait_for_consistent_cluster(Ports) ->
    ered_test_utils:wait_for_consistent_cluster(Ports, ?CLIENT_OPTS).

end_per_suite(_Config) ->
    stop_containers().

stop_containers() ->
    %% Stop containers. redis-30007 is used in t_new_cluster_master and
    %% redis-cluster for running redis-cli in create_cluster.
    cmd_log([io_lib:format("docker stop redis-~p; docker rm redis-~p;", [P, P])
             || P <- ?PORTS ++ [30007, cluster]]).


t_command(_) ->
    R = start_cluster(),
    lists:foreach(fun(N) ->
                          {ok, <<"OK">>} = ered:command(R, [<<"SET">>, N, N], N)
                  end,
                  [integer_to_binary(N) || N <- lists:seq(1,100)]),
    no_more_msgs().


t_command_async(_) ->
    R = start_cluster(),
    Pid = self(),
    ReplyFun = fun(Reply) -> Pid ! Reply end,
    ered:command_async(R, [<<"SET">>, <<"hello">>, <<"joe">>], <<"hello">>, ReplyFun),
    ered:command_async(R, [<<"GET">>, <<"hello">>], <<"hello">>, ReplyFun),
    ered:command_async(R, [<<"DEL">>, <<"hello">>], <<"hello">>, ReplyFun),
    receive {ok, <<"OK">>}  -> ok after 1000 -> error(timeout) end,
    receive {ok, <<"joe">>} -> ok after 1000 -> error(timeout) end,
    receive {ok, 1}         -> ok after 1000 -> error(timeout) end,
    no_more_msgs().


t_command_all(_) ->
    R = start_cluster(),
    [{ok, <<"PONG">>}, {ok, <<"PONG">>}, {ok, <<"PONG">>}] = ered:command_all(R, [<<"PING">>]),
    no_more_msgs().


t_command_client(_) ->
    R = start_cluster(),
    lists:foreach(fun(N) ->
                          {ok, <<"OK">>} = ered:command(R, [<<"SET">>, N, N], N)
                  end,
                  [integer_to_binary(N) || N <- lists:seq(1,100)]),
    Keys = lists:foldl(fun(Client, Acc) ->
                               scan_helper(Client, <<"0">>, Acc)
                       end,
                       [],
                       ered:get_clients(R)),
    Match = lists:seq(1,100),
    Match = lists:sort([binary_to_integer(N) || N <- lists:flatten(Keys)]),
    no_more_msgs().


t_command_pipeline(_) ->
    R = start_cluster(),
    Cmds = [[<<"SET">>, <<"{k}1">>, <<"1">>], [<<"SET">>, <<"{k}2">>, <<"2">>]],
    {ok, [<<"OK">>, <<"OK">>]} = ered:command(R, Cmds, <<"k">>),
    no_more_msgs().


t_cluster_crash(_) ->
    {cluster, Pid} = start_cluster(),
    exit(Pid, crash),
    ?MSG(#{msg_type := cluster_stopped, reason := crash}),
    no_more_msgs().


t_client_crash(_) ->
    R = start_cluster(),
    Port = get_master_from_key(R, <<"k">>),
    Addr = {"127.0.0.1", Port},
    AddrToClient0 = ered:get_addr_to_client_map(R),
    {client, Pid0} = maps:get(Addr, AddrToClient0),
    monitor(process, Pid0),
    TestPid = self(),
    SleepCommand = [<<"DEBUG">>, <<"SLEEP">>, <<"1">>],
    spawn_link(fun () ->
                       Result = ered:command(R, SleepCommand, <<"k">>),
                       TestPid ! {crashed_command_result, Result}
               end),
    ered:command_async(R, SleepCommand, <<"k">>,
                       fun (Reply) ->
                               TestPid ! {crashed_async_command_result, Reply}
                       end),
    timer:sleep(100),
    exit(Pid0, crash),
    ?MSG({crashed_async_command_result,{error,{client_stopped,crash}}}),
    ?MSG({crashed_command_result,{error,{client_stopped,crash}}}),
    ?MSG(#{addr := {"127.0.0.1", Port}, master := true, msg_type := client_stopped}),
    ?MSG({'DOWN', _Mon, process, Pid0, crash}),
    ?MSG(#{msg_type := cluster_not_ok, reason := master_down}),
    %% Send command when the client process is dead. The cluster process
    %% starts a new client synchronously, so the command succeeds. There's a
    %% possible race condition here though. The cluster process may receive the
    %% command before it receives the 'DOWN' message from the dead client and
    %% thus it doesn't know the client is dead.
    {ok, <<"OK">>} = ered:command(R, [<<"SET">>, <<"k">>, <<"v">>], <<"k">>),
    ered:command_async(R, [<<"SET">>, <<"k">>, <<"v">>], <<"k">>,
                       fun (Reply) ->
                               %% This command does get a reply.
                               TestPid ! {async_command_when_down, Reply}
                       end),
    ?MSG({async_command_when_down, {ok, <<"OK">>}}),
    %% End of race condition.
    ?MSG(#{addr := {"127.0.0.1", Port}, master := true, msg_type := connected}, 10000),
    AddrToClient1 = ered:get_addr_to_client_map(R),
    {client, Pid1} = maps:get(Addr, AddrToClient1),
    true = (Pid1 =/= Pid0),
    {ok, <<"OK">>} = ered:command(R, [<<"SET">>, <<"k">>, <<"v">>], <<"k">>),
    ?MSG(#{msg_type := cluster_ok}),
    no_more_msgs().


t_client_killed(_) ->
    %% This test case simulates the scenario that a client process crashes
    %% before a command has been added to the waiting queue. It's a race
    %% condition, but by killing the process hard and preventing terminate/2
    %% from calling reply_all/2, we achieve a similar result.
    R = start_cluster(),
    Port = get_master_from_key(R, <<"k">>),
    Addr = {"127.0.0.1", Port},
    AddrToClient0 = ered:get_addr_to_client_map(R),
    {client, Pid0} = maps:get(Addr, AddrToClient0),
    monitor(process, Pid0),
    TestPid = self(),
    SleepCommand = [<<"DEBUG">>, <<"SLEEP">>, <<"1">>],
    spawn_link(fun () ->
                       Result = ered:command(R, SleepCommand, <<"k">>),
                       TestPid ! {crashed_command_result, Result}
               end),
    timer:sleep(200), %% Let the spawned command be sent.
    ered:command_async(R, SleepCommand, <<"k">>,
                       fun (Reply) ->
                               %% We never get this reply. The ered_client
                               %% process crashes before it is sent.
                               TestPid ! {crashed_async_command_result, Reply}
                       end),
    exit(Pid0, kill),
    ?MSG({crashed_command_result, {error, killed}}),
    ?MSG({'DOWN', _Mon, process, Pid0, killed}),
    %% We don't get 'cluster_not_ok' here, because ered_cluster relies on a
    %% message from ered_client. Using a monitor instead would be more reliable.

    %% Send command when the client process is dead. The cluster process
    %% starts a new client synchronously, so the command succeeds. There's a
    %% possible race condition here though. The cluster process may receive the
    %% command before it receives the 'DOWN' message from the dead client and
    %% thus it doesn't know the client is dead.
    {ok, <<"OK">>} = ered:command(R, [<<"SET">>, <<"k">>, <<"v">>], <<"k">>),
    ered:command_async(R, [<<"SET">>, <<"k">>, <<"v">>], <<"k">>,
                       fun (Reply) ->
                               %% This command does get a reply.
                               TestPid ! {async_command_when_down, Reply}
                       end),
    ?MSG({async_command_when_down, {ok, <<"OK">>}}),
    %% End of race condition.
    ?MSG(#{addr := {"127.0.0.1", Port}, master := true, msg_type := connected}, 10000),
    AddrToClient1 = ered:get_addr_to_client_map(R),
    {client, Pid1} = maps:get(Addr, AddrToClient1),
    true = (Pid1 =/= Pid0),
    {ok, <<"OK">>} = ered:command(R, [<<"SET">>, <<"k">>, <<"v">>], <<"k">>),
    %% We don't get message 'cluster_ok' because we never got 'cluster_not_ok'.
    no_more_msgs().


t_hard_failover(_) ->
    R = start_cluster([{min_replicas, 1}]),
    Port = get_master_port(R),
    Pod = get_pod_name_from_port(Port),
    ct:pal("~p\n", [ered:command_all(R, [<<"CLUSTER">>, <<"SLOTS">>])]),
    ct:pal(os:cmd("docker stop " ++ Pod)),

    ?MSG(#{msg_type := socket_closed, addr := {"127.0.0.1", Port}, reason := {recv_exit, closed}}),
    ?MSG(#{msg_type := connect_error, addr := {"127.0.0.1", Port}, reason := econnrefused}),

    ?MSG(#{msg_type := node_down_timeout, addr := {"127.0.0.1", Port}}, 2500),
    ?MSG(#{msg_type := cluster_not_ok, reason := master_down}),

    ?MSG(#{msg_type := slot_map_updated}, 5000),
    ?MSG(#{msg_type := node_deactivated, addr := {"127.0.0.1", Port}}),

    ct:pal("~p\n", [ered:command_all(R, [<<"CLUSTER">>, <<"SLOTS">>])]),

    ct:pal(os:cmd("docker start " ++ Pod)),

    %% node back: first the initial add reconnects
    ?MSG(#{msg_type := connected, addr := {"127.0.0.1", Port}, master := false}, 10000),

    %% new slotmap when old master comes up as replica
    ?MSG(#{msg_type := slot_map_updated}, 10000),

    %% OK when cluster convergence check is done
    ?MSG(#{msg_type := cluster_ok}, 5000),

    %% Ignore any previous failed reconnect attempts
    ?OPTIONAL_MSG(#{msg_type := connect_error, addr := {"127.0.0.1", Port}, reason := econnrefused}),
    ?OPTIONAL_MSG(#{msg_type := connect_error, addr := {"127.0.0.1", Port}, reason := econnrefused}),
    no_more_msgs().

do_manual_failover(R) ->
    [Client|_] = ered:get_clients(R),
    {ok, SlotMap} = ered:command_client(Client, [<<"CLUSTER">>, <<"SLOTS">>]),

    ct:pal("~p\n", [SlotMap]),
    %% Get the port of a replica node
    [[_SlotStart, _SlotEnd, [_, OldMasterPort |_], [_Ip, Port |_] | _] | _] = SlotMap,

    %% sometimes the manual failover is not successful so loop until it is
    fun Loop() ->
            "OK\n" = os:cmd("redis-cli -p " ++ integer_to_list(Port) ++ " CLUSTER FAILOVER"),
            %% Wait for failover to start, otherwise the commands might be sent
            %% too early to detect it.
            fun Wait(0) ->
                    Loop();
                Wait(N) ->
                    timer:sleep(500),
                    Role = os:cmd("redis-cli -p " ++ integer_to_list(Port) ++ " ROLE"),
                    ct:pal("~p\n", [Role]),
                    case lists:prefix("master", Role) of
                        true ->
                            true;
                        false ->
                            Wait(N-1)
                    end
            end(10)
    end(),
    {OldMasterPort, Port}.

t_manual_failover(_) ->
    R = start_cluster(),
    {_OldMasterPort, Port} = do_manual_failover(R),
    lists:foreach(fun(N) ->
                          {ok, _} = ered:command(R, [<<"SET">>, N, N], N)
                  end,
                  [integer_to_binary(N) || N <- lists:seq(1,1000)]),
    ct:pal("~p\n", [os:cmd("redis-cli -p " ++ integer_to_list(Port) ++ " ROLE")]),

    ?MSG(#{msg_type := slot_map_updated}),
    no_more_msgs().

t_manual_failover_then_old_master_down(_) ->
    %% Check that if a manual failover is triggered and the old master is taken
    %% down before ered was triggered to update the slotmap, ered still doesn't
    %% report 'cluster_not_ok'.
    R = start_cluster([{min_replicas, 0}]),
    {Port, NewMasterPort} = do_manual_failover(R),

    %% Failover done. Now shutdown the old master and stop the container. The
    %% connection to it is lost. Ered still believes it's a master because it
    %% has not yet learnt about the failover.
    cmd_log(io_lib:format("redis-cli -p ~p SHUTDOWN", [Port])),
    cmd_log(io_lib:format("docker stop redis-~p", [Port])),
    ?MSG(#{addr := {"127.0.0.1", Port},
           master := true,
           msg_type := socket_closed,
           reason := {recv_exit, closed}}),

    %% Ered prefers the replica of the disconnected node for slot map update,
    %% since it is likely to know about a failover first; it is the new master.
    ?MSG(#{msg_type := slot_map_updated, addr := {"127.0.0.1", NewMasterPort}}),

    %% Wait for the cluster to become consistent without the stopped node.
    wait_for_consistent_cluster(?PORTS -- [Port]),

    %% Start container and wait for it to come up.
    cmd_log(io_lib:format("docker start redis-~p", [Port])),
    timer:sleep(2000),

    %% Wait until the cluster is consistent again.
    wait_for_consistent_cluster(),

    %% Wait for ered to reconnect to the restarted node.
    ?MSG(#{msg_type := connected,
           addr := {"127.0.0.1", Port},
           master := false}),

    %% We allow a number of info messages during this procedure, but not the
    %% status change events 'cluster_not_ok' and 'cluster_ok'.
    ?OPTIONAL_MSG(#{addr := {"127.0.0.1", Port}, msg_type := connect_error, reason := econnrefused}),
    ?OPTIONAL_MSG(#{addr := {"127.0.0.1", Port}, msg_type := connect_error, reason := econnrefused}),
    ?OPTIONAL_MSG(#{addr := {"127.0.0.1", Port}, msg_type := node_down_timeout}),
    ?OPTIONAL_MSG(#{addr := {"127.0.0.1", Port}, msg_type := node_deactivated}),
    no_more_msgs().

t_blackhole(_) ->
    %% Simulate that a Redis node is unreachable, e.g. a network failure. We use
    %% 'docker pause', similar to sending SIGSTOP to a process, to make one
    %% Redis node unresponsive. This makes TCP recv() and connect() time out.
    CloseWait = 2000,                           % default is 10000
    NodeDownTimeout = 2000,                     % default is 2000
    ResponseTimeout = 10000,                    % default is 10000
    R = start_cluster([{close_wait, CloseWait},
                       %% Require no replicas for 'cluster OK'.
                       {min_replicas, 0},
                       {client_opts,
                        [{node_down_timeout, NodeDownTimeout},
                         {connection_opts,
                          [{response_timeout, ResponseTimeout}]}]}
                      ]),
    Port = get_master_port(R),
    Pod = get_pod_name_from_port(Port),
    ClientRef = maps:get({"127.0.0.1", Port}, ered:get_addr_to_client_map(R)),
    ct:pal("Pausing container: " ++ os:cmd("docker pause " ++ Pod)),

    %% Now when a command times out, the ered_connection process is restarted
    %% but connect() times out. Ered reports that one is down, thus the cluster
    %% is not OK. Failover happens. Ered discovers the new master and reports
    %% that the cluster is OK again.
    TestPid = self(),
    ered:command_client_async(ClientRef, [<<"PING">>],
                              fun(Reply) -> TestPid ! {ping_reply, Reply} end),

    ?MSG(#{msg_type := socket_closed, reason := {recv_exit, timeout}, master := true},
         ResponseTimeout + 1000),
    ?MSG({ping_reply, {error, _Reason}}, % node_down or node_deactivated
         NodeDownTimeout + 1000),
    ?MSG(#{msg_type := slot_map_updated},
         5000),
    ?OPTIONAL_MSG(#{msg_type := node_down_timeout, addr := {"127.0.0.1", Port}}),
    ?OPTIONAL_MSG(#{msg_type := cluster_not_ok, reason := master_down}),
    ?MSG(#{msg_type := node_deactivated, addr := {"127.0.0.1", Port}}),
    ?OPTIONAL_MSG(#{msg_type := cluster_ok}),
    ?MSG(#{msg_type := client_stopped, reason := shutdown, master := false},
         CloseWait + 1000),

    ct:pal("Unpausing container: " ++ os:cmd("docker unpause " ++ Pod)),
    timer:sleep(500),
    wait_for_consistent_cluster(),

    %% Since we have {min_replicas, 0} in this testcase, the slot map is not
    %% updated repeatedly until replicas are found. Instead, we use the
    %% opportunity to test manual slot update here.
    ered:update_slots(R),
    ?MSG(#{msg_type := slot_map_updated}),
    ?MSG(#{msg_type := connected, addr := {"127.0.0.1", Port}, master := false}),

    no_more_msgs().

t_blackhole_all_nodes(_) ->
    %% Simulate that all nodes are unreachable, e.g. a network failure. We use
    %% 'docker pause', similar to sending SIGSTOP to a process, to make the
    %% nodes unresponsive. This makes TCP recv() and connect() time out.
    CloseWait = 2000,                           % default is 10000
    NodeDownTimeout = 2000,                     % default is 2000
    ResponseTimeout = 10000,                    % default is 10000
    R = start_cluster([{close_wait, CloseWait},
                       %% Require replicas for 'cluster OK'.
                       {min_replicas, 1},
                       {client_opts,
                        [{node_down_timeout, NodeDownTimeout},
                         {connection_opts,
                          [{response_timeout, ResponseTimeout}]}]}
                      ]),

    %% Pause all nodes
    lists:foreach(fun(Port) ->
                          Pod = get_pod_name_from_port(Port),
                          ct:pal("Pausing container: " ++ os:cmd("docker pause " ++ Pod))
                  end, ?PORTS),

    %% Send PING to all nodes and expect closed sockets, error replies for sent requests,
    %% and a report that the cluster is not ok.
    TestPid = self(),
    AddrToClient = ered:get_addr_to_client_map(R),
    maps:foreach(fun(_ClientAddr, ClientRef) ->
                         ered:command_client_async(ClientRef, [<<"PING">>],
                                                   fun(Reply) -> TestPid ! {ping_reply, Reply} end)
                 end, AddrToClient),

    [?MSG(#{msg_type := socket_closed, reason := {recv_exit, timeout}, addr := {"127.0.0.1", Port}},
          ResponseTimeout + 1000) || Port <- ?PORTS],
    ?MSG({ping_reply, {error, _Reason1}}, NodeDownTimeout + 1000),
    ?MSG({ping_reply, {error, _Reason2}}, NodeDownTimeout + 1000),
    ?MSG({ping_reply, {error, _Reason3}}, NodeDownTimeout + 1000),
    ?MSG({ping_reply, {error, _Reason4}}, NodeDownTimeout + 1000),
    ?MSG({ping_reply, {error, _Reason5}}, NodeDownTimeout + 1000),
    ?MSG({ping_reply, {error, _Reason6}}, NodeDownTimeout + 1000),
    [?MSG(#{msg_type := node_down_timeout, addr := {"127.0.0.1", Port}}) || Port <- ?PORTS],
    ?MSG(#{msg_type := cluster_not_ok, reason := master_down}),

    %% Unpause all nodes
    lists:foreach(fun(Port) ->
                          Pod = get_pod_name_from_port(Port),
                          ct:pal("Unpausing container: " ++ os:cmd("docker unpause " ++ Pod))
                  end, ?PORTS),
    timer:sleep(500),

    wait_for_consistent_cluster(),

    %% Expect connects and a cluster ok.
    [?MSG(#{msg_type := connected, addr := {"127.0.0.1", Port}}, 10000) || Port <- ?PORTS],
    ?MSG(#{msg_type := cluster_ok}, 10000),

    no_more_msgs().


t_connect_timeout(_) ->
    %% Connect to an unreachable cluster (a blackhole address) using a configured
    %% connect_timeout.
    {ok, _P} = ered:connect_cluster([{"192.168.254.254", 30001}],
                                    [{info_pid, [self()]},
                                     {client_opts,
                                      [{connection_opts, [{connect_timeout, 100}]}]
                                     }]),

    ?MSG(#{msg_type := connect_error, reason := timeout}, 200),
    no_more_msgs().


t_init_timeout(_) ->
    Opts = [
            {client_opts,
             [{connection_opts, [{response_timeout, 1000}]}]
            }
           ],
    ct:pal("~p\n", [os:cmd("redis-cli -p 30001 CLIENT PAUSE 10000")]),
    {ok, _P} = ered:connect_cluster([{localhost, 30001}], [{info_pid, [self()]}] ++ Opts),

    ?MSG(#{msg_type := socket_closed, reason := {recv_exit, timeout}}, 3500),
    ?MSG(#{msg_type := node_down_timeout, addr := {localhost, 30001}}, 2500),
    %% Does not work on  Redis before 6.2.0.
    ct:pal("~p\n", [os:cmd("redis-cli -p 30001 CLIENT UNPAUSE")]),

    ?MSG(#{msg_type := connected, addr := {localhost, 30001}}),

    ?MSG(#{msg_type := slot_map_updated}),

    ?MSG(#{msg_type := node_deactivated, addr := {localhost, 30001}}),
    ?MSG(#{msg_type := connected, addr := {"127.0.0.1", 30001}}),
    ?MSG(#{msg_type := connected, addr := {"127.0.0.1", 30002}}),
    ?MSG(#{msg_type := connected, addr := {"127.0.0.1", 30003}}),
    ?MSG(#{msg_type := connected, addr := {"127.0.0.1", 30004}}),
    ?MSG(#{msg_type := connected, addr := {"127.0.0.1", 30005}}),
    ?MSG(#{msg_type := connected, addr := {"127.0.0.1", 30006}}),

    ?MSG(#{msg_type := cluster_ok}),
    no_more_msgs().


t_empty_slotmap(_) ->
    R = start_cluster(),
    reset_cluster(),
    {ok, {error, <<"CLUSTERDOWN Hash slot not served">>}} =
        ered:command(R, [<<"GET">>, <<"hello">>], <<"hello">>),
    ?MSG(#{msg_type := cluster_slots_error_response,
           response := empty,
           addr := {"127.0.0.1", _Port}}),
    create_cluster(),
    wait_for_consistent_cluster(),
    no_more_msgs().


t_empty_initial_slotmap(_) ->
    reset_cluster(),
    {ok, R} = ered:connect_cluster([{"127.0.0.1", 30001}],
                                   [{info_pid, [self()]}, {min_replicas, 1}]),
    ?MSG(#{msg_type := cluster_slots_error_response,
           response := empty,
           addr := {"127.0.0.1", 30001}}),
    {error, unmapped_slot} =
        ered:command(R, [<<"GET">>, <<"hello">>], <<"hello">>),

    %% Now restore the cluster and check that ered reaches an OK state.
    create_cluster(),
    wait_for_consistent_cluster(),

    %% Ered updates the slotmap repeatedly until all slots are covered and all
    %% masters have a replica. In the end, we're connected to all nodes.
    [?MSG(#{msg_type := connected, addr := {"127.0.0.1", Port}}, 10000)
     || Port <- ?PORTS],
    ?MSG(#{msg_type := cluster_ok}, 5000),

    %% Ignore all slotmap updates. There may be multiple of those before all
    %% nodes have discovered each other. There may be incomplete slotmaps as
    %% well before all nodes have discovered each other, so the connections to
    %% some nodes may be temporarily deactivated.
    ?OPTIONAL_MSG(#{msg_type := slot_map_updated}),
    ?OPTIONAL_MSG(#{msg_type := slot_map_updated}),
    ?OPTIONAL_MSG(#{msg_type := slot_map_updated}),
    ?OPTIONAL_MSG(#{msg_type := slot_map_updated}),
    ?OPTIONAL_MSG(#{msg_type := slot_map_updated}),
    ?OPTIONAL_MSG(#{msg_type := slot_map_updated}),
    ?OPTIONAL_MSG(#{msg_type := node_deactivated}),
    ?OPTIONAL_MSG(#{msg_type := node_deactivated}),

    no_more_msgs().


t_scan_delete_keys(_) ->
    R = start_cluster(),
    Clients = ered:get_clients(R),
    [{ok, _} = ered:command_client(Client, [<<"FLUSHDB">>]) || Client <- Clients],

    Keys = [iolist_to_binary([<<"key">>, integer_to_binary(N)]) || N <- lists:seq(1,10000)],
    [{ok, _} = ered:command(R, [<<"SET">>, K, <<"dummydata">>], K) || K <- Keys],

    Keys2 = [iolist_to_binary([<<"otherkey">>, integer_to_binary(N)]) || N <- lists:seq(1,100)],
    [{ok, _} = ered:command(R, [<<"SET">>, K, <<"dummydata">>], K) || K <- Keys2],

    %% selectively delete the otherkey ones
    %% (this will lead to some SCAN responses being empty, good case to test)
    Pid = self(),
    [spawn_link(fun() -> Pid ! scan_delete(Client) end) || Client <- Clients],
    [receive ok -> ok end || _ <- Clients],

    Size = [ered:command_client(Client,[<<"DBSIZE">>]) || Client <- Clients],
    10000 = lists:sum([N || {ok, N} <- Size]).




scan_delete(Client) ->
    scan_cmd(Client, <<"0">>),
    fun Loop(_OutstandingUnlink=0, _ScanDone=true) ->
            ok;
        Loop(OutstandingUnlink, ScanDone) ->
            receive
                {scan, {ok, [<<"0">> ,Keys]}} ->
                    Loop(OutstandingUnlink + unlink_cmd(Client, Keys), _ScanDone=true);

                {scan, {ok, [Cursor, Keys]}} ->
                    scan_cmd(Client, Cursor),
                    Loop(OutstandingUnlink + unlink_cmd(Client, Keys), ScanDone);

                {unlink, {ok, Result}} when is_list(Result) ->
                    Loop(OutstandingUnlink-1, ScanDone);

                Other ->
                    exit({error, Other})
            end
    end(0, false).


scan_cmd(Client, Cursor) ->
    Pid = self(),
    Callback = fun(Response) -> Pid ! {scan, Response} end,
    ered:command_client_async(Client, [<<"SCAN">>, Cursor, <<"COUNT">>, integer_to_binary(100), <<"MATCH">>, <<"otherkey*">>], Callback).

unlink_cmd(_Client, []) ->
    0;
unlink_cmd(Client, Keys) ->
    Pid = self(),
    Callback = fun(Response) -> Pid ! {unlink, Response} end,
    ered:command_client_async(Client, [[<<"UNLINK">>, Key] || Key <- Keys], Callback),
    1.

group_by_hash(Keys) ->
    lists:foldl(fun(Key, Acc) ->
                        Fun = fun(KeysForSlot) -> [Key|KeysForSlot] end,
                        maps:update_with(ered_lib:hash(Key), Fun, _Init = [], Acc)
                end,
                #{},
                Keys).


%% Test to send and receive a big data packet. The read should be split
t_split_data(_) ->
    Data = iolist_to_binary([<<"A">> || _ <- lists:seq(0,3000)]),
    R = start_cluster(),
    {ok, <<"OK">>} = ered:command(R, [<<"SET">>, <<"key1">>, Data], <<"key1">>),
    {ok, Data} = ered:command(R, [<<"GET">>, <<"key1">>], <<"key1">>),
    ok.


t_subscribe(_) ->
    TestPid = self(),
    PushCb = fun(Push) -> TestPid ! {push, Push} end,
    Opts = [{client_opts, [{connection_opts, [{push_cb, PushCb}]}]}],
    R = start_cluster(Opts),

    %% Error response
    {ok, {error, <<"ERR wrong number of arguments", _/binary>>}} =
        ered:command(R, [<<"subscribe">>], <<"k">>),

    %% Subscribe multiple channels.
    {ok, undefined} = ered:command(R, [<<"subscribe">>, <<"ch1">>, <<"ch2">>, <<"ch3">>, <<"ch4">>], <<"k">>),
    {ok, <<"PONG">>} = ered:command(R, [<<"ping">>], <<"k">>),
    ?MSG({push, [<<"subscribe">>, <<"ch1">>, 1]}),
    ?MSG({push, [<<"subscribe">>, <<"ch2">>, 2]}),
    ?MSG({push, [<<"subscribe">>, <<"ch3">>, 3]}),
    ?MSG({push, [<<"subscribe">>, <<"ch4">>, 4]}),

    %% Publish to the same as where we're subscribed.
    {ok, 1} = ered:command(R, [<<"publish">>, <<"ch2">>, <<"hello">>], <<"k">>),
    ?MSG({push, [<<"message">>, <<"ch2">>, <<"hello">>]}),

    %% Publish to a different node; not to the one where we're subscribed.
    {ok, 0} = ered:command(R, [<<"publish">>, <<"ch2">>, <<"world">>], <<"x">>),
    ?MSG({push, [<<"message">>, <<"ch2">>, <<"world">>]}),

    %% Psubscribe (channel counter is sum of patterns, channels and
    %% shard-channels)
    {ok, undefined} = ered:command(R, [<<"psubscribe">>, <<"ch*">>], <<"k">>),
    ?MSG({push, [<<"psubscribe">>, <<"ch*">>, 5]}),

    %% Unsubscribe some.
    {ok, undefined} = ered:command(R, [<<"unsubscribe">>, <<"ch2">>, <<"ch1">>], <<"k">>),
    ?MSG({push, [<<"unsubscribe">>, _Ch1, 4]}),
    ?MSG({push, [<<"unsubscribe">>, _Ch2, 3]}),

    %% Unsubscribe all.
    {ok, undefined} = ered:command(R, [<<"unsubscribe">>], <<"k">>),
    ?MSG({push, [<<"unsubscribe">>, _Ch3, 2]}),
    ?MSG({push, [<<"unsubscribe">>, _Ch4, 1]}),

    %% Punsubscribe all.
    {ok, undefined} = ered:command(R, [<<"punsubscribe">>], <<"k">>),
    ?MSG({push, [<<"punsubscribe">>, <<"ch*">>, 0]}),

    %% Sharded pubsub in Redis 7+. May return a MOVED redirect, but here it is CROSSSLOT.
    {ok, {error, Reason}} = ered:command(R, [<<"ssubscribe">>, <<"a">>, <<"b">>], <<"a">>),
    case Reason of
        <<"CROSSSLOT", _/binary>> -> ok;          % Redis 7+
        <<"ERR unknown command", _/binary>> -> ok % Redis 6
    end,
    no_more_msgs().


t_queue_full(_) ->
    ct:pal("~s\n", [os:cmd("redis-cli -p 30001 INFO")]),

    Opts = [{max_pending, 10}, {max_waiting, 10}, {queue_ok_level, 5}, {node_down_timeout, 10000}],
    Client = start_cluster([{client_opts, Opts}]),
    Ports = [30001, 30002, 30003, 30004, 30005, 30006],
    [os:cmd("redis-cli -p " ++ integer_to_list(Port) ++ " CLIENT PAUSE 2000") || Port <- Ports],
    [C|_] = ered:get_clients(Client),
    Pid = self(),

    fun Loop(0) -> ok;
        Loop(N) -> ered:command_client_async(C, [<<"PING">>], fun(Reply) -> Pid ! {reply, Reply} end),
                   Loop(N-1)
    end(21),

    ?MSG({reply, {error, queue_overflow}}),
    [ct:pal("~s\n", [os:cmd("redis-cli -p " ++ integer_to_list(Port) ++ " CLIENT UNPAUSE")]) || Port <- Ports],
    ?MSG(#{msg_type := queue_full}),
    ?MSG(#{msg_type := cluster_not_ok, reason := master_queue_full}),

    ?MSG(#{msg_type := queue_ok}),
    ?MSG(#{msg_type := cluster_ok}),
    [?MSG({reply, {ok, <<"PONG">>}}) || _ <- lists:seq(1,20)],
    no_more_msgs(),
    ok.

t_kill_client(_) ->
    R = start_cluster(),
    Port = get_master_port(R),

    %% KILL will close the TCP connection to the redis client
    ct:pal("~p\n",[os:cmd("redis-cli -p " ++ integer_to_list(Port) ++ " CLIENT KILL TYPE NORMAL")]),
    ?MSG(#{msg_type := socket_closed, addr := {_, Port}}),

    %% connection reestablished
    ?MSG(#{msg_type := connected, addr := {_, Port}}),
    no_more_msgs().

t_new_cluster_master(_) ->
    R = start_cluster([{min_replicas, 0},
                       {close_wait, 100}]),

    %% Create new master
    Image = os:getenv("SERVER_DOCKER_IMAGE", ?DEFAULT_SERVER_DOCKER_IMAGE),
    Pod = cmd_log("docker run --name redis-30007 -d --net=host --restart=on-failure "++Image++" redis-server --cluster-enabled yes --port 30007 --cluster-node-timeout 2000"),
    cmd_until("redis-cli -p 30007 CLUSTER MEET 127.0.0.1 30001", "OK"),
    cmd_until("redis-cli -p 30007 CLUSTER INFO", "cluster_state:ok"),

    %% Set some dummy data
    Key = <<"test_key">>,
    Data = <<"dummydata">>,
    {ok, _} = ered:command(R, [<<"SET">>, Key, Data], Key),


    %% Find what node owns the data now. Get a move redirecton and extract port. Ex: MOVED 5798 172.100.0.1:6392
    Moved = cmd_log("redis-cli -p 30007 GET " ++ binary_to_list(Key)),
    SourcePort = string:trim(lists:nth(2, string:split(Moved, ":"))),
    DestPort = "30007",

    cmd_until("redis-cli -p 30007 CLUSTER MEET 127.0.0.1 " ++ SourcePort, "OK"),
    %% Move the data to new master
    move_key(SourcePort, DestPort, Key),

    %% Make sure it moved
    cmd_until("redis-cli -p 30007 GET " ++ binary_to_list(Key), binary_to_list(Data)),
    timer:sleep(2000),
    %% Fetch with client. New connection should be opened and new slot map update
    {ok, Data} = ered:command(R, [<<"GET">>, Key], Key),
    ?MSG(#{msg_type := slot_map_updated}, 10000),
    ?MSG(#{msg_type := connected, addr := {"127.0.0.1",30007}}),

    no_more_msgs(),
    cmd_log("redis-cli -p "++ SourcePort ++" CLUSTER SLOTS"),
    %% Move back the slot
    move_key(DestPort, SourcePort, Key),

    cmd_until("redis-cli -p "++ SourcePort ++" GET " ++ binary_to_list(Key), binary_to_list(Data)),
    cmd_log("redis-cli -p "++ SourcePort ++" CLUSTER SLOTS"),

    %% Sleep here otherwise the cluster slots timer will still be active and the move will not trigger
    %% a slot map update
    timer:sleep(1000),
    {ok, Data} = ered:command(R, [<<"GET">>, Key], Key),
    ?MSG(#{msg_type := slot_map_updated}, 5000),

    %% Handling of the now unused node depends on Redis version.
    %% In Redis 7+ the node is removed from the slot map.
    receive
        #{msg_type := node_deactivated} -> ?MSG(#{msg_type := client_stopped}) % Redis 7+
    after 1000 ->
            %% In Redis v6.2 the node becomes a replica, i.e. it is not removed from the slot map.
            %% A manual remove is required in v6.2
            NewNodeId = string:trim(cmd_log("redis-cli -p 30007 CLUSTER MYID")),
            lists:foreach(fun(Port) ->
                                  cmd_until("redis-cli -p "++ integer_to_list(Port) ++" CLUSTER FORGET "++ NewNodeId, "OK")
                          end, ?PORTS),
            %% Update slotmap by picking a specific node to avoid using port 30007.
            %% The node using port 30007 includes itself in the slotmap and dont
            %% accept CLUSTER FORGET. This avoids intermittent missing messages.
            ClientRef = maps:get({"127.0.0.1", 30001}, ered:get_addr_to_client_map(R)),
            ered:update_slots(R, ClientRef),
            ?MSG(#{msg_type := slot_map_updated}),
            ?MSG(#{msg_type := node_deactivated}),
            ?MSG(#{msg_type := client_stopped})
    end,

    cmd_log("docker stop " ++ Pod),

    %% Verify that the cluster is still ok
    {ok, Data} = ered:command(R, [<<"GET">>, Key], Key),
    ered:close(R),
    ?MSG(#{msg_type := cluster_stopped, reason := normal}),
    no_more_msgs().

t_ask_redirect(_) ->
    R = start_cluster(),

    %% Clear all old data
    Clients = ered:get_clients(R),
    [{ok, _} = ered:command_client(Client, [<<"FLUSHDB">>]) || Client <- Clients],

    Key = <<"test_key">>,

    SourcePort = integer_to_list(get_master_from_key(R, Key)),
    DestPort = integer_to_list(hd([Port || Port <- get_all_masters(R), integer_to_list(Port) /= SourcePort])),

    Slot = integer_to_list(ered_lib:hash(Key)),
    SourceNodeId = string:trim(cmd_log("redis-cli -p " ++ SourcePort ++ " CLUSTER MYID")),
    DestNodeId = string:trim(cmd_log("redis-cli -p " ++ DestPort ++ " CLUSTER MYID")),

    %% Set "{test_key}1" in MIGRATING node
    {ok,<<"OK">>} = ered:command(R, [<<"SET">>, <<"{test_key}1">>, <<"DATA1">>], Key),

    cmd_log("redis-cli -p " ++ DestPort ++ " CLUSTER SETSLOT " ++ Slot ++ " IMPORTING " ++ SourceNodeId),
    cmd_log("redis-cli -p " ++ SourcePort ++ " CLUSTER SETSLOT " ++ Slot ++ " MIGRATING " ++ DestNodeId),

    %% Test single command. unknown key leads to ASK redirection
    {ok,undefined} = ered:command(R, [<<"GET">>, Key], Key),

    %% Test multiple commands. unknown key leads to ASK redirection. The {test_key}2 will be set
    %% in IMPORTING node after command
    {ok,[undefined,<<"PONG">>,<<"OK">>]} = ered:command(R,
                                                        [[<<"GET">>, <<"{test_key}2">>],
                                                         [<<"PING">>] ,
                                                         [<<"SET">>, <<"{test_key}2">>, <<"DATA2">>]],
                                                        Key),

    %% The keys are set in different nodes. {test_key}2 needs an ASK redirect to retrieve the value
    %% {test_key}1 is in the MIGRATING node
    %% {test_key}2 is in the IMPORTING node
    %% {test_key}3 is not set
    {ok,[<<"DATA1">>, <<"DATA2">>, undefined]} = ered:command(R,
                                                              [[<<"GET">>, <<"{test_key}1">>],
                                                               [<<"GET">>, <<"{test_key}2">>],
                                                               [<<"GET">>, <<"{test_key}3">>]],
                                                              Key),

    %% A command with several keys with partial keys in the MIGRATING node will trigger a TRYAGAIN error
    %% {test_key}1 is in the MIGRATING node
    %% {test_key}2 is in the IMPORTING node

    %% Why the ASK error here then? According to https://redis.io/commands/cluster-setslot/ documentation
    %% the MIGRATING node should trigger a TRYAGAIN. But when this is tested with redis_version:6.0.8 it leads
    %% to an ASK answer. ASKING the IMPORTING node will lead to a TRYAGAIN that will send the command back
    %% to the MIGRATING node. When the attempt retrys run out the final reply will be from the MIGRATING node.
    %% If attempts are set to an uneven number the final reply will be TRYAGAIN from the IMPORTING node.
    %% Since the ASK redirect does not trigger the TRYAGAIN delay this means the time the client waits before
    %% giving up in a TRYAGAIN scenario is effectively cut in half.
    {ok,{error,Reason}} = ered:command(R,
                                       [<<"MGET">>,
                                        <<"{test_key}1">>,
                                        <<"{test_key}2">>],
                                       Key),
    case Reason of
        <<"TRYAGAIN", _/binary>> -> ok;  % Redis 7+
        <<"ASK", _/binary>> -> ok        % Redis 6
    end,

    %% Try the running the same command again but trigger a MIGRATE to move {test_key}1 to the IMPORTING node.
    %% This should lead to the command returning the data for both keys once it asks the correct node.
    Pid = self(),

    %% run the command async
    spawn_link(fun() ->
                       Reply = ered:command(R,
                                            [<<"MGET">>,
                                             <<"{test_key}1">>,
                                             <<"{test_key}2">>],
                                            Key),
                       Pid ! {the_reply, Reply}
               end),
    %% wait a bit to trigger the migration to let the client attempt to fetch and get redirected
    timer:sleep(200),
    cmd_log("redis-cli -p " ++ SourcePort ++ " MIGRATE 127.0.0.1 " ++ DestPort ++ " \"\" 0 5000 KEYS {test_key}1"),

    {ok,[<<"DATA1">>,<<"DATA2">>]} =  receive {the_reply, Reply} -> Reply after 5000 -> error(no_reply) end,

    %% So far there should not be any new slot map update
    no_more_msgs(),

    %% Finalize the migration
    cmd_log("redis-cli -p " ++ DestPort ++ " CLUSTER SETSLOT " ++ Slot ++ " NODE " ++ DestNodeId),
    cmd_log("redis-cli -p " ++ SourcePort ++ " CLUSTER SETSLOT " ++ Slot ++ " NODE "++ DestNodeId),

    %% Now this should lead to a moved redirection and a slotmap update
    {ok,undefined} = ered:command(R, [<<"GET">>, Key], Key),
    ?MSG(#{msg_type := slot_map_updated}, 1000),
    no_more_msgs(),

    %% now restore the slot
    %% remove these, cant be bothered to move them back
    ered:command(R, [<<"DEL">>,  <<"{test_key}1">>, <<"{test_key}2">>], Key),
    move_key(DestPort, SourcePort, Key),
    {ok,undefined} = ered:command(R, [<<"GET">>, Key], Key),
    ?MSG(#{msg_type := slot_map_updated}, 1000),

    %% wait a bit to let the config spread to all nodes
    timer:sleep(5000),
    ok.

%% Remove a slot. Make sure that we don't get 'cluster_ok' until the slot is
%% covered again. Redis =< 7 has a bug that gossips back a deleted slot to the
%% last seen owner of the slot.
t_missing_slot(_) ->
    R = start_cluster(),
    Key = <<"foo">>,
    Slot = ered_lib:hash(Key),
    Port = get_master_from_key(R, Key),
    Addr = {"127.0.0.1", Port},
    AddrToClient = ered:get_addr_to_client_map(R),
    Client = maps:get(Addr, AddrToClient),

    cmd_log("redis-cli -p " ++ integer_to_list(Port) ++ " CLUSTER DELSLOTS " ++ integer_to_list(Slot)),
    ered:update_slots(R, Client),
    ?MSG(#{msg_type := cluster_not_ok, reason := not_all_slots_covered}),
    timer:sleep(6000),
    ?MSG(#{msg_type := slot_map_updated}),
    ?OPTIONAL_MSG(#{msg_type := slot_map_updated}),
    ?OPTIONAL_MSG(#{msg_type := slot_map_updated}),
    no_more_msgs(),
    cmd_log("redis-cli -p " ++ integer_to_list(Port) ++ " CLUSTER ADDSLOTS " ++ integer_to_list(Slot)),
    ?MSG(#{msg_type := cluster_ok}, 5000),
    ?OPTIONAL_MSG(#{msg_type := slot_map_updated}),
    no_more_msgs(),
    ok.

t_client_map(_) ->
    R = start_cluster(),
    Expected = [{"127.0.0.1", Port} || Port <- [30001, 30002, 30003, 30004, 30005, 30006]],
    Map = ered:get_addr_to_client_map(R),
    Expected = lists:sort(maps:keys(Map)).


move_key(SourcePort, DestPort, Key) ->
    Slot = integer_to_list(ered_lib:hash(Key)),
    SourceNodeId = string:trim(cmd_log("redis-cli -p " ++ SourcePort ++ " CLUSTER MYID")),
    DestNodeId = string:trim(cmd_log("redis-cli -p " ++ DestPort ++ " CLUSTER MYID")),

    cmd_log("redis-cli -p " ++ DestPort ++ " CLUSTER SETSLOT " ++ Slot ++ " IMPORTING " ++ SourceNodeId),
    cmd_log("redis-cli -p " ++ SourcePort ++ " CLUSTER SETSLOT " ++ Slot ++ " MIGRATING " ++ DestNodeId),
    cmd_log("redis-cli -p " ++ SourcePort ++ " MIGRATE 127.0.0.1 " ++ DestPort ++ " " ++ binary_to_list(Key) ++ " 0 5000"),

    cmd_log("redis-cli -p " ++ DestPort ++ " CLUSTER SETSLOT " ++ Slot ++ " NODE " ++ DestNodeId),
    cmd_log("redis-cli -p " ++ SourcePort ++ " CLUSTER SETSLOT " ++ Slot ++ " NODE " ++ DestNodeId).



start_cluster() ->
    start_cluster([]).
start_cluster(Opts) ->
    ered_test_utils:start_cluster(?PORTS, Opts).

no_more_msgs() ->
    {messages,Msgs} = erlang:process_info(self(), messages),
    case  Msgs of
        [] ->
            ok;
        Msgs ->
            error({unexpected,Msgs})
    end.


scan_helper(Client, Curs0, Acc0) ->
    {ok, [Curs1, Keys]} = ered:command_client(Client, [<<"SCAN">>, Curs0]),
    Acc1 = [Keys|Acc0],
    case Curs1 of
        <<"0">> ->
            Acc1;
        _ ->
            scan_helper(Client, Curs1, Acc1)
    end.

cmd_log(Cmd) ->
    R = os:cmd(Cmd),
    ct:pal("~s\n~s\n", [Cmd, R]),
    R.

cmd_until(Cmd, Regex) ->
    fun Loop(N) ->
            case re:run(cmd_log(Cmd), Regex) of
                nomatch when N > 0 ->
                    timer:sleep(500),
                    Loop(N -1);
                nomatch when N =< 0 ->
                    error({timeout_cmd_until, Cmd, Regex});
                Match ->
                    Match
            end
    end(100).


get_all_masters(R) ->
    [Port || [_SlotStart, _SlotEnd, [_Ip, Port| _] | _] <- get_slot_map(R)].

get_master_from_key(R, Key) ->
    Slot = ered_lib:hash(Key),
    hd([Port || [SlotStart, SlotEnd, [_Ip, Port| _] | _] <- get_slot_map(R),
                SlotStart =< Slot,
                Slot =< SlotEnd]).


get_slot_map(R) ->
    [Client|_] = ered:get_clients(R),
    {ok, SlotMap} = ered:command_client(Client, [<<"CLUSTER">>, <<"SLOTS">>]),
    SlotMap.

get_master_port(R) ->
    [[_SlotStart, _SlotEnd, [_Ip, Port |_] | _] | _] = get_slot_map(R),
    Port.

get_pod_name_from_port(Port) ->
    "redis-" ++ integer_to_list(Port).
