-module(ered_SUITE).

-compile([export_all, nowarn_export_all]).

all() ->
    [
     t_command,
     t_command_async,
     t_command_all,
     t_command_client,
     t_command_pipeline,
     t_scan_delete_keys,
     t_hard_failover,
     t_manual_failover,
     t_manual_failover_then_old_master_down,
     t_blackhole,
     t_init_timeout,
     t_empty_slotmap,
     t_empty_initial_slotmap,
     t_split_data,
     t_subscribe,
     t_queue_full,
     t_kill_client,
     t_new_cluster_master,
     t_ask_redirect,
     t_client_map
    ].


-define(MSG(Pattern, Timeout),
        receive
            Pattern -> ok
        after
            Timeout -> error({timeout, ??Pattern, erlang:process_info(self(), messages)})
        end).

-define(MSG(Pattern), ?MSG(Pattern, 1000)).

-define(OPTIONAL_MSG(Pattern),
        receive
            Pattern -> ok
        after
            0 -> ok
        end).

-define(PORTS, [30001, 30002, 30003, 30004, 30005, 30006]).

-define(DEFAULT_REDIS_DOCKER_IMAGE, "redis:6.2.7").

init_per_suite(_Config) ->
    stop_containers(), % just in case there is junk from previous runs
    Image = os:getenv("REDIS_DOCKER_IMAGE", ?DEFAULT_REDIS_DOCKER_IMAGE),
    cmd_log([io_lib:format("docker run --name redis-~p -d --net=host"
                           " --restart=on-failure ~s redis-server"
                           " --cluster-enabled yes --port ~p"
                           " --cluster-node-timeout 2000;",
                           [P, Image, P])
             || P <- ?PORTS]),

    timer:sleep(2000),
    lists:foreach(fun(Port) ->
                          {ok,Pid} = ered_client:start_link("127.0.0.1", Port, []),
                          {ok, <<"PONG">>} = ered_client:command(Pid, <<"ping">>),
                          ered_client:stop(Pid)
                  end, ?PORTS),

    create_cluster(),
    wait_for_consistent_cluster(),
    [].

init_per_testcase(_Testcase, Config) ->
    %% Quick check that cluster is OK; otherwise restart everything.
    case catch check_consistent_cluster(?PORTS) of
        ok ->
            [];
        _ ->
            ct:pal("Re-initialize the cluster"),
            init_per_suite(Config)
    end.

create_cluster() ->
    Image = os:getenv("REDIS_DOCKER_IMAGE", ?DEFAULT_REDIS_DOCKER_IMAGE),
    Hosts = [io_lib:format("127.0.0.1:~p ", [P]) || P <- ?PORTS],
    Cmd = io_lib:format("echo 'yes' | "
                        "docker run --name redis-cluster --rm --net=host -i ~s "
                        "redis-cli --cluster-replicas 1 --cluster create ~s",
                        [Image, Hosts]),
    cmd_log(Cmd).

reset_cluster() ->
    Pids = [begin
                {ok, Pid} = ered_client:start_link("127.0.0.1", Port, []),
                Pid
            end || Port <- ?PORTS],
    [{ok, <<"OK">>} = ered_client:command(Pid, [<<"CLUSTER">>, <<"RESET">>]) || Pid <- Pids],
    [{ok, []} = ered_client:command(Pid, [<<"CLUSTER">>, <<"SLOTS">>]) || Pid <- Pids],
    lists:foreach(fun ered_client:stop/1, Pids).

%% Wait until cluster is consistent, i.e all nodes have the same single view
%% of the slot map and all cluster nodes are included in the slot map.
wait_for_consistent_cluster() ->
    wait_for_consistent_cluster(?PORTS).

wait_for_consistent_cluster(Ports) ->
    fun Loop(N) ->
            case check_consistent_cluster(Ports) of
                ok ->
                    true;
                {error, _} when N > 0 ->
                    timer:sleep(500),
                    Loop(N-1);
                {error, SlotMaps} ->
                    error({timeout_consistent_cluster, SlotMaps})
            end
    end(20).

check_consistent_cluster(Ports) ->
    SlotMaps = [fun(Port) ->
                        {ok, Pid} = ered_client:start_link("127.0.0.1", Port, []),
                        {ok, SlotMap} = ered_client:command(Pid, [<<"CLUSTER">>, <<"SLOTS">>]),
                        ered_client:stop(Pid),
                        SlotMap
                end(P) || P <- Ports],
    Consistent = case lists:usort(SlotMaps) of
                     [SlotMap] ->
                         Ports =:= [Port || {_Ip, Port} <- ered_lib:slotmap_all_nodes(SlotMap)];
                     _NotAllIdentical ->
                         false
                 end,
    case Consistent of
        true -> ok;
        false -> {error, SlotMaps}
    end.

end_per_suite(_Config) ->
    stop_containers().

stop_containers() ->
    %% Stop containers, redis-30007 used in t_new_cluster_master
    cmd_log([io_lib:format("docker stop redis-~p; docker rm redis-~p;", [P, P])
             || P <- ?PORTS ++ [30007]]).


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

    %% a client is already connected to the node, so cluster is ok immediately
    ?MSG(#{msg_type := cluster_ok}),

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
    ?OPTIONAL_MSG(#{addr := {"127.0.0.1", Port}, msg_type := connect_error}),
    ?OPTIONAL_MSG(#{addr := {"127.0.0.1", Port}, msg_type := node_down_timeout}),
    ?OPTIONAL_MSG(#{addr := {"127.0.0.1", Port}, msg_type := node_deactivated}),
    no_more_msgs().

t_blackhole(_) ->
    %% Simulate that a Redis node is unreachable, e.g. a network failure. We use
    %% 'docket pause', similar to sending SIGSTOP to a process, to make one
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
    ClientPid = maps:get({"127.0.0.1", Port}, ered:get_addr_to_client_map(R)),
    ct:pal("Pausing container: " ++ os:cmd("docker pause " ++ Pod)),

    %% Now when a command times out, the ered_connection process is restarted
    %% but connect() times out. Ered reports that one is down, thus the cluster
    %% is not OK. Failover happens. Ered discovers the new master and reports
    %% that the cluster is OK again.
    TestPid = self(),
    ered:command_client_async(ClientPid, [<<"PING">>],
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
    ?MSG(#{msg_type := client_stopped, reason := normal, master := false},
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


t_init_timeout(_) ->
    Opts = [
            {client_opts,
             [{connection_opts, [{response_timeout, 1000}]}]
            }
           ],
    ct:pal("~p\n", [os:cmd("redis-cli -p 30001 CLIENT PAUSE 10000")]),
    {ok, _P} = ered:start_link([{localhost, 30001}], [{info_pid, [self()]}] ++ Opts),

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
    no_more_msgs().


t_empty_initial_slotmap(_) ->
    reset_cluster(),
    {ok, R} = ered:start_link([{"127.0.0.1", 30001}],
                              [{info_pid, [self()]}, {min_replicas, 1}]),
    ?MSG(#{msg_type := cluster_slots_error_response,
           response := empty,
           addr := {"127.0.0.1", 30001}}),
    {error, unmapped_slot} =
        ered:command(R, [<<"GET">>, <<"hello">>], <<"hello">>),

    %% Now restore the cluster and check that ered reaches an OK state.
    create_cluster(),

    %% Ered updates the slotmap repeatedly until all slots are covered and all
    %% masters have a replica. In the end, we're connected to all nodes.
    [?MSG(#{msg_type := connected, addr := {"127.0.0.1", Port}}, 10000)
     || Port <- ?PORTS],
    ?MSG(#{msg_type := cluster_ok}),

    %% Ingore all slotmap updates. There may be multiple of those before all
    %% nodes have discovered each other. There may be incomplete slotmaps as
    %% well before all nodes have discovered each other, so the connections to
    %% some nodes may be temporarily deactivated.
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

    recv({reply, {error, queue_overflow}}, 1000),
    [ct:pal("~s\n", [os:cmd("redis-cli -p " ++ integer_to_list(Port) ++ " CLIENT UNPAUSE")]) || Port <- Ports],
    msg(msg_type, queue_full),
    #{reason := master_queue_full} = msg(msg_type, cluster_not_ok),


    msg(msg_type, queue_ok),
    msg(msg_type, cluster_ok),
    [recv({reply, {ok, <<"PONG">>}}, 1000) || _ <- lists:seq(1,20)],
    no_more_msgs(),
    ok.

t_kill_client(_) ->
    R = start_cluster(),
    Port = get_master_port(R),

    %% KILL will close the TCP connection to the redis client
    ct:pal("~p\n",[os:cmd("redis-cli -p " ++ integer_to_list(Port) ++ " CLIENT KILL TYPE NORMAL")]),
    #{addr := {_, Port}} = msg(msg_type, socket_closed),

    %% connection reestablished
    #{addr := {_, Port}} = msg(msg_type, connected),
    no_more_msgs().

t_new_cluster_master(_) ->
    R = start_cluster([{min_replicas, 0},
                       {close_wait, 100}]),

    %% Create new master
    Image = os:getenv("REDIS_DOCKER_IMAGE", ?DEFAULT_REDIS_DOCKER_IMAGE),
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
            ClientPid = maps:get({"127.0.0.1", 30001}, ered:get_addr_to_client_map(R)),
            ered:update_slots(R, ClientPid),
            ?MSG(#{msg_type := slot_map_updated}),
            ?MSG(#{msg_type := node_deactivated}),
            ?MSG(#{msg_type := client_stopped})
    end,

    cmd_log("docker stop " ++ Pod),

    %% Verify that the cluster is still ok
    {ok, Data} = ered:command(R, [<<"GET">>, Key], Key),
    ered:stop(R),
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
    [Port1, Port2 | PortsRest] = Ports = ?PORTS,
    InitialNodes = [{"127.0.0.1", Port} || Port <- [Port1, Port2]],

    wait_for_consistent_cluster(),
    {ok, P} = ered:start_link(InitialNodes, [{info_pid, [self()]}] ++ Opts),

    ConnectedInit = [#{msg_type := connected} = msg(addr, {"127.0.0.1", Port})
                     || Port <- [Port1, Port2]],

    #{slot_map := SlotMap} = msg(msg_type, slot_map_updated, 1000),

    IdMap =  maps:from_list(lists:flatmap(
                              fun([_,_|Nodes]) ->
                                      [{Port, Id} || [_Addr, Port, Id |_]<- Nodes]
                              end, SlotMap)),

    ConnectedRest = [#{msg_type := connected} = msg(addr, {"127.0.0.1", Port})
                     || Port <- PortsRest],

    ClusterIds = [Id || #{cluster_id := Id} <- ConnectedInit ++ ConnectedRest],
    ClusterIds = [maps:get(Port, IdMap) || Port <- Ports],

    ?MSG(#{msg_type := cluster_ok}),

    %% Clear all old data
    [{ok, _} = ered:command_client(Client, [<<"FLUSHDB">>]) || Client <- ered:get_clients(P)],

    no_more_msgs(),
    P.

msg(Key, Val) ->
    msg(Key, Val, 1000).

msg(Key, Val, Time) ->
    receive
        M = #{Key := Val} -> M
    after Time ->
            error({timeout, {Key, Val}, erlang:process_info(self(), messages)})
    end.

recv(Msg, Time) ->
    receive
        Msg -> Msg
    after Time ->
            error({timeout, Msg, erlang:process_info(self(), messages)})
    end.

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
