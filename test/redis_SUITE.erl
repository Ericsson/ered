-module(redis_SUITE).


%export([init_per_suite/1, end_per_suite/1]).

-compile([export_all]).

all() ->
    [
     t_command,
     t_command_all,
     t_command_client,
     t_command_pipeline,
     t_scan_delete_keys,
     t_hard_failover,
     t_manual_failover,
     t_init_timeout,
     t_split_data,
     t_queue_full,
     t_kill_client,
     t_new_cluster_master,
     t_ask_redirect
    ].


-define(MSG(Pattern, Timeout),
        receive
            Pattern -> ok
        after
            Timeout -> error({timeout, ??Pattern, erlang:process_info(self(), messages)})
        end).

-define(MSG(Pattern), ?MSG(Pattern, 1000)).

init_per_suite(Config) ->
    %% TODO use port_command so we can get the exit code here?
    R =  os:cmd("docker run --name redis-1 -d --net=host --restart=on-failure redis redis-server --cluster-enabled yes --port 30001 --cluster-node-timeout 2000;"
                "docker run --name redis-2 -d --net=host --restart=on-failure redis redis-server --cluster-enabled yes --port 30002 --cluster-node-timeout 2000;"
                "docker run --name redis-3 -d --net=host --restart=on-failure redis redis-server --cluster-enabled yes --port 30003 --cluster-node-timeout 2000;"
                "docker run --name redis-4 -d --net=host --restart=on-failure redis redis-server --cluster-enabled yes --port 30004 --cluster-node-timeout 2000;"
                "docker run --name redis-5 -d --net=host --restart=on-failure redis redis-server --cluster-enabled yes --port 30005 --cluster-node-timeout 2000;"
                "docker run --name redis-6 -d --net=host --restart=on-failure redis redis-server --cluster-enabled yes --port 30006 --cluster-node-timeout 2000;"),
    ct:pal(R),
    timer:sleep(1000),
    lists:foreach(fun(Port) ->
                          {ok,Pid} = redis_client:start_link("127.0.0.1", Port, []),
                          {ok, <<"PONG">>} = redis_client:command(Pid, <<"ping">>)
                  end,
                  [30001, 30002, 30003, 30004, 30005, 30006]),
    os:cmd(" echo 'yes' | docker run --name redis-cluster --net=host -i redis redis-cli --cluster create 127.0.0.1:30001 127.0.0.1:30002 127.0.0.1:30003 127.0.0.1:30004 127.0.0.1:30005 127.0.0.1:30006 --cluster-replicas 1"),

    %% Wait for cluster to be fully up. If not then we get a "CLUSTERDOWN The cluster is down" error when sending commands.
    %% TODO set cluster-require-full-coverage to no and see if it helps
    timer:sleep(5000),
    [].

end_per_suite(Config) ->
    os:cmd("docker stop redis-cluster; docker rm redis-cluster;"
           "docker stop redis-1; docker rm redis-1;"
           "docker stop redis-2; docker rm redis-2;"
           "docker stop redis-3; docker rm redis-3;"
           "docker stop redis-4; docker rm redis-4;"
           "docker stop redis-5; docker rm redis-5;"
           "docker stop redis-6; docker rm redis-6;"
           "docker stop redis-7; docker rm redis-7"). % redis-7 used in t_new_cluster_master


t_command(_) ->
    R = start_cluster(),
    lists:foreach(fun(N) ->
                          {ok, <<"OK">>} = redis:command(R, [<<"SET">>, N, N], N)
                  end,
                  [integer_to_binary(N) || N <- lists:seq(1,100)]),
    no_more_msgs().


t_command_all(_) ->
    R = start_cluster(),
    [{ok, <<"PONG">>}, {ok, <<"PONG">>}, {ok, <<"PONG">>}] = redis:command_all(R, [<<"PING">>]),
    no_more_msgs().


t_command_client(_) ->
    R = start_cluster(),
    lists:foreach(fun(N) ->
                          {ok, <<"OK">>} = redis:command(R, [<<"SET">>, N, N], N)
                  end,
                  [integer_to_binary(N) || N <- lists:seq(1,100)]),
    Keys = lists:foldl(fun(Client, Acc) ->
                               scan_helper(Client, <<"0">>, Acc)
                       end,
                       [],
                       redis:get_clients(R)),
    Match = lists:seq(1,100),
    Match = lists:sort([binary_to_integer(N) || N <- lists:flatten(Keys)]),
    no_more_msgs().


t_command_pipeline(_) ->
    R = start_cluster(),
    Cmds = [[<<"SET">>, <<"{k}1">>, <<"1">>], [<<"SET">>, <<"{k}2">>, <<"2">>]],
    {ok, [<<"OK">>, <<"OK">>]} = redis:command(R, Cmds, <<"k">>),
    no_more_msgs().


t_hard_failover(_) ->
    R = start_cluster([{close_wait, 2000}]),
    Port = get_master_port(R),
    Pod = get_pod_name_from_port(Port),
    ct:pal("~p\n", [redis:command_all(R, [<<"CLUSTER">>, <<"SLOTS">>])]),
    ct:pal(os:cmd("docker stop " ++ Pod)),

    ?MSG(#{msg_type := socket_closed, addr := {localhost, Port}, reason := {recv_exit, closed}}),
    ?MSG(#{msg_type := socket_closed, addr := {"127.0.0.1", Port}, reason := {recv_exit, closed}}),

    ?MSG(#{msg_type := cluster_not_ok, reason := master_down}),
    ?MSG(#{msg_type := connect_error, addr := {localhost, Port}, reason := econnrefused}),
    ?MSG(#{msg_type := connect_error, addr := {"127.0.0.1", Port}, reason := econnrefused}),


    ?MSG(#{msg_type := slot_map_updated}, 5000),

    ct:pal("~p\n", [redis:command_all(R, [<<"CLUSTER">>, <<"SLOTS">>])]),

    ct:pal(os:cmd("docker start " ++ Pod)),

    %% node back: first the inital add reconnects
    ?MSG(#{msg_type := connected, addr := {localhost, Port}}, 10000),

    %% new slotmap when old master comes up as replica
    ?MSG(#{msg_type := slot_map_updated}, 10000),

    %% new client comes up
    ?MSG(#{msg_type := connected, addr := {"127.0.0.1", Port}, master := false}, 10000),
    ?MSG(#{msg_type := cluster_ok}),

    %% old client to the failed node is closed since it was removed from slotmap
    ?MSG(#{msg_type := client_stopped, addr := {"127.0.0.1", Port}, reason := normal}, 2000),

    no_more_msgs().

t_manual_failover(_) ->
    R = start_cluster(),
    SlotMaps = redis:command_all(R, [<<"CLUSTER">>, <<"SLOTS">>]),
    ct:pal("~p\n", [SlotMaps]),
    [Client|_] = redis:get_clients(R),
    {ok, SlotMap} = redis:command_client(Client, [<<"CLUSTER">>, <<"SLOTS">>]),

    ct:pal("~p\n", [SlotMap]),
    %% Get the port of a replica node
    [[_SlotStart, _SlotEnd, _Master, [_Ip, Port |_] | _] | _] = SlotMap,

    %% sometimes the manual failover is not successful so loop until it is
    fun Loop() ->
            "OK\n" = os:cmd("redis-cli -p " ++ integer_to_list(Port) ++ " CLUSTER FAILOVER"),
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

    %% Wait for failover to start, otherwise the commands might be sent to early to detect
    lists:foreach(fun(N) ->
                          {ok, _} = redis:command(R, [<<"SET">>, N, N], N)
                  end,
                  [integer_to_binary(N) || N <- lists:seq(1,1000)]),
    ct:pal("~p\n", [os:cmd("redis-cli -p " ++ integer_to_list(Port) ++ " ROLE")]),

    ?MSG(#{msg_type := slot_map_updated}),
    no_more_msgs().


t_init_timeout(_) ->
    Opts = [
            {client_opts,
             [{connection_opts, [{response_timeout, 1000}]}]
            }
           ],
    ct:pal("~p\n", [os:cmd("redis-cli -p 30001 CLIENT PAUSE 10000")]),
    {ok, P} = redis:start_link([{localhost, 30001}], [{info_pid, [self()]}] ++ Opts),

    ?MSG(#{msg_type := socket_closed, reason := {recv_exit, timeout}}, 3500),
    %% Does not work on  Redis before 6.2.0.
    ct:pal("~p\n", [os:cmd("redis-cli -p 30001 CLIENT UNPAUSE")]),

    ?MSG(#{msg_type := connected, addr := {localhost, 30001}}),

    ?MSG(#{msg_type := slot_map_updated}),

    ?MSG(#{msg_type := connected, addr := {"127.0.0.1", 30001}}),
    ?MSG(#{msg_type := connected, addr := {"127.0.0.1", 30002}}),
    ?MSG(#{msg_type := connected, addr := {"127.0.0.1", 30003}}),
    ?MSG(#{msg_type := connected, addr := {"127.0.0.1", 30004}}),
    ?MSG(#{msg_type := connected, addr := {"127.0.0.1", 30005}}),
    ?MSG(#{msg_type := connected, addr := {"127.0.0.1", 30006}}),

    ?MSG(#{msg_type := cluster_ok}),
    no_more_msgs().


t_scan_delete_keys(_) ->
    R = start_cluster(),
    Clients = redis:get_clients(R),
    [{ok, _} = redis:command_client(Client, [<<"FLUSHDB">>]) || Client <- Clients],

    Keys = [iolist_to_binary([<<"key">>, integer_to_binary(N)]) || N <- lists:seq(1,10000)],
    [{ok, _} = redis:command(R, [<<"SET">>, K, <<"dummydata">>], K) || K <- Keys],

    Keys2 = [iolist_to_binary([<<"otherkey">>, integer_to_binary(N)]) || N <- lists:seq(1,100)],
    [{ok, _} = redis:command(R, [<<"SET">>, K, <<"dummydata">>], K) || K <- Keys2],

    %% selectivly delete the otherkey ones
    %% (this will lead to some SCAN responses being empty, good case to test)
    Pid = self(),
    [spawn_link(fun() -> Pid ! scan_delete(Client) end) || Client <- Clients],
    [receive ok -> ok end || _ <- Clients],

    Size = [redis:command_client(Client,[<<"DBSIZE">>]) || Client <- Clients],
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
    redis:command_client_async(Client, [<<"SCAN">>, Cursor, <<"COUNT">>, integer_to_binary(100), <<"MATCH">>, <<"otherkey*">>], Callback).

unlink_cmd(Client, []) ->
    0;
unlink_cmd(Client, Keys) ->
    Pid = self(),
    Callback = fun(Response) -> Pid ! {unlink, Response} end,
    redis:command_client_async(Client, [[<<"UNLINK">>, Key] || Key <- Keys], Callback),
    1.

group_by_hash(Keys) ->
    lists:foldl(fun(Key, Acc) ->
                        Fun = fun(KeysForSlot) -> [Key|KeysForSlot] end,
                        maps:update_with(redis_lib:hash(Key), Fun, _Init = [], Acc)
                end,
                #{},
                Keys).


%% Test to send and receive a big data packet. The read should be split
t_split_data(_) ->
    Data = iolist_to_binary([<<"A">> || _ <- lists:seq(0,3000)]),
    R = start_cluster(),
    {ok, <<"OK">>} = redis:command(R, [<<"SET">>, <<"key1">>, Data], <<"key1">>),
    {ok, Data} = redis:command(R, [<<"GET">>, <<"key1">>], <<"key1">>),
    ok.



t_queue_full(_) ->
    ct:pal("~p\n", [os:cmd("redis-cli -p 30001 INFO")]),

    Opts = [{max_pending, 10}, {max_waiting, 10}, {queue_ok_level, 5}, {node_down_timeout, 10000}],
    Client = start_cluster([{client_opts, Opts}]),
    Ports = [30001, 30002, 30003, 30004, 30005, 30006],
    [os:cmd("redis-cli -p " ++ integer_to_list(Port) ++ " CLIENT PAUSE 2000") || Port <- Ports],
    [C|_] = redis:get_clients(Client),
    Pid = self(),

    fun Loop(0) -> ok;
        Loop(N) -> redis:command_client_async(C, [<<"PING">>], fun(Reply) -> Pid ! {reply, Reply} end),
                   Loop(N-1)
    end(21),

    recv({reply, {error, queue_overflow}}, 1000),
    [ct:pal("~p\n", [os:cmd("redis-cli -p " ++ integer_to_list(Port) ++ " CLIENT UNPAUSE")]) || Port <- Ports],
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
    #{addr := {_, Port}} = msg(msg_type, socket_closed),

    %% connection reestablished
    #{addr := {_, Port}} = msg(msg_type, connected),
    #{addr := {_, Port}} = msg(msg_type, connected),
    no_more_msgs().

t_new_cluster_master(_) ->
    R = start_cluster([{min_replicas, 0},
                       {close_wait, 100}]),

    %% Create new master
    cmd_log("docker run --name redis-7 -d --net=host --restart=on-failure redis redis-server --cluster-enabled yes --port 30007 --cluster-node-timeout 2000"),
    cmd_until("redis-cli -p 30007 CLUSTER MEET 127.0.0.1 30001", "OK"),
    cmd_until("redis-cli -p 30007 CLUSTER INFO", "cluster_state:ok"),

    %% Clear all old data
    Clients = redis:get_clients(R),
    [{ok, _} = redis:command_client(Client, [<<"FLUSHDB">>]) || Client <- Clients],

    %% Set some dummy data
    Key = <<"test_key">>,
    Data = <<"dummydata">>,
    {ok, _} = redis:command(R, [<<"SET">>, Key, Data], Key),


    %% Find what node owns the data now. Get a move redirecton and extract port. Ex: MOVED 5798 172.100.0.1:6392
    Moved = cmd_log("redis-cli -p 30007 GET " ++ binary_to_list(Key)),
    SourcePort = string:trim(lists:nth(2, string:split(Moved, ":"))),
    DestPort = "30007",
    %% Move the data to new master
    move_key(SourcePort, DestPort, Key),

    %% Make sure it moved
    cmd_until("redis-cli -p 30007 GET " ++ binary_to_list(Key), binary_to_list(<<"dummydata">>)),

    %% Fetch with client. New connection should be opened and new slot map update
    {ok, Data} = redis:command(R, [<<"GET">>, Key], Key),
    ?MSG(#{msg_type := slot_map_updated}, 10000),
    %% The new node is still connecting so there is a cluster_not_ok here. Maybe the cluster logic should
    %% be changed so that the cluster is not reported as nok until a connect fails. On the other hand it is
    %% good to only be in cluster_ok when everything us up and running fully.
    ?MSG(#{msg_type := cluster_not_ok,reason := master_down}),
    ?MSG(#{msg_type := connected, addr := {"127.0.0.1",30007}}),
    ?MSG(#{msg_type := cluster_ok}),
    %% Move back the slot
    move_key(DestPort, SourcePort, Key),

    Pod = get_pod_name_from_port(30007),
    cmd_log("docker stop " ++ Pod),

    ?MSG(#{msg_type := socket_closed}),
    ?MSG(#{msg_type := connect_error}),
    ?MSG(#{msg_type := cluster_not_ok,reason := master_down}),
    ?MSG(#{msg_type := slot_map_updated}, 5000),
    ?MSG(#{msg_type := cluster_ok}),
    ?MSG(#{msg_type := client_stopped}),
    no_more_msgs().

t_ask_redirect(_) ->
    R = start_cluster(),

    %% Clear all old data
    Clients = redis:get_clients(R),
    [{ok, _} = redis:command_client(Client, [<<"FLUSHDB">>]) || Client <- Clients],

    Key = <<"test_key">>,

    %Moved = cmd_log("redis-cli -p 30001 GET " ++ binary_to_list(Key)),

    SourcePort = integer_to_list(get_master_from_key(R, Key)),
    DestPort = integer_to_list(hd([Port || Port <- get_all_masters(R), integer_to_list(Port) /= SourcePort])),

    Slot = integer_to_list(redis_lib:hash(Key)),
    SourceNodeId = string:trim(cmd_log("redis-cli -p " ++ SourcePort ++ " CLUSTER MYID")),
    DestNodeId = string:trim(cmd_log("redis-cli -p " ++ DestPort ++ " CLUSTER MYID")),

    %% Set "{test_key}1" in MIGRATING node
    {ok,<<"OK">>} = redis:command(R, [<<"SET">>, <<"{test_key}1">>, <<"DATA1">>], Key),

    cmd_log("redis-cli -p " ++ DestPort ++ " CLUSTER SETSLOT " ++ Slot ++ " IMPORTING " ++ SourceNodeId),
    cmd_log("redis-cli -p " ++ SourcePort ++ " CLUSTER SETSLOT " ++ Slot ++ " MIGRATING " ++ DestNodeId),

    %% Test single command. unknown key leads to ASK redirection
    {ok,undefined} = redis:command(R, [<<"GET">>, Key], Key),

    %% Test multiple commands. unknown key leads to ASK redirection. The {test_key}2 will be set
    %% in IMPORTING node after command
    {ok,[undefined,<<"PONG">>,<<"OK">>]} = redis:command(R,
                                                         [[<<"GET">>, <<"{test_key}2">>],
                                                          [<<"PING">>] ,
                                                          [<<"SET">>, <<"{test_key}2">>, <<"DATA2">>]],
                                                         Key),

    %% The keys are set in different nodes. {test_key}2 needs an ASK redirect to retrive the value
    %% {test_key}1 is in the MIGRATING node
    %% {test_key}2 is in the IMPORTING node
    %% {test_key}3 is not set
    {ok,[<<"DATA1">>, <<"DATA2">>, undefined]} = redis:command(R,
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
    %% If attemts are set to an uneven number the final reply will be TRYAGAIN from the IMPORTING node.
    %% Since the ASK redirect does not trigger the TRYAGAIN delay this means the time the client waits before
    %% giving up in a TRYAGAIN scenario is effectively cut in half.
    {ok,{error,<<"ASK", _/binary>>}} = redis:command(R,
                                                     [<<"MGET">>,
                                                      <<"{test_key}1">>,
                                                      <<"{test_key}2">>],
                                                     Key),

    %% Try the running the same command again but trigger a MIGRATE to move {test_key}1 to the IMPORTING node.
    %% This should lead to the command returning the data for both keys once it asks the correct node. 
    Pid = self(),

    %% run the command async
    spawn_link(fun() ->
                       Reply = redis:command(R,
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
    cmd_log("redis-cli -p " ++ DestPort ++ " CLUSTER SETSLOT " ++ Slot ++ " STABLE"),
    cmd_log("redis-cli -p " ++ SourcePort ++ " CLUSTER SETSLOT " ++ Slot ++ " STABLE"),

    cmd_log("redis-cli -p " ++ DestPort ++ " CLUSTER SETSLOT " ++ Slot ++ " NODE " ++ DestNodeId),
    cmd_log("redis-cli -p " ++ SourcePort ++ " CLUSTER SETSLOT " ++ Slot ++ " NODE "++ DestNodeId),

    %% Now this should lead to a moved redirection and a slotmap update
    {ok,undefined} = redis:command(R, [<<"GET">>, Key], Key),
    #{slot_map := SlotMap} = msg(msg_type, slot_map_updated, 1000),
    no_more_msgs(),
    ok.




move_key(SourcePort, DestPort, Key) ->
    Slot = integer_to_list(redis_lib:hash(Key)),
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
    Ports = [30001, 30002, 30003, 30004, 30005, 30006],
    InitialNodes = [{localhost, Port} || Port <- Ports],

    {ok, P} = redis:start_link(InitialNodes, [{info_pid, [self()]}] ++ Opts),

    ?MSG(#{msg_type := connected, addr := {localhost, 30001}}),
    ?MSG(#{msg_type := connected, addr := {localhost, 30002}}),
    ?MSG(#{msg_type := connected, addr := {localhost, 30003}}),
    ?MSG(#{msg_type := connected, addr := {localhost, 30004}}),
    ?MSG(#{msg_type := connected, addr := {localhost, 30005}}),
    ?MSG(#{msg_type := connected, addr := {localhost, 30006}}),


    #{slot_map := SlotMap} = msg(msg_type, slot_map_updated, 1000),

    IdMap =  maps:from_list(lists:flatmap(
                              fun([_,_|Nodes]) ->
                                      [{Port, Id} || [_Addr, Port, Id |_]<- Nodes]
                              end, SlotMap)),

    R = [#{msg_type := connected} = msg(addr, {"127.0.0.1", Port}) || Port <- Ports],

    ClusterIds = [Id || #{cluster_id := Id} <- R],
    ClusterIds = [maps:get(Port, IdMap) || Port <- Ports],

    ?MSG(#{msg_type := cluster_ok}),

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
    {ok, [Curs1, Keys]} = redis:command_client(Client, [<<"SCAN">>, Curs0]),
    Acc1 = [Keys|Acc0],
    case Curs1 of
        <<"0">> ->
            Acc1;
        _ ->
            scan_helper(Client, Curs1, Acc1)
    end.

cmd_log(Cmd) ->
    R = os:cmd(Cmd),
    ct:pal("~p -> ~s\n", [Cmd, R]),
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
    end(20).


get_all_masters(R) ->
    [Port || [SlotStart, SlotEnd, [_Ip, Port| _] | _] <- get_slot_map(R)].

get_master_from_key(R, Key) ->
    Slot = redis_lib:hash(Key),
    hd([Port || [SlotStart, SlotEnd, [_Ip, Port| _] | _] <- get_slot_map(R), 
                SlotStart =< Slot,
                Slot =< SlotEnd]).


get_slot_map(R) ->
    [Client|_] = redis:get_clients(R),
    {ok, SlotMap} = redis:command_client(Client, [<<"CLUSTER">>, <<"SLOTS">>]),
    SlotMap.

get_master_port(R) ->
    [[_SlotStart, _SlotEnd, [_Ip, Port |_] | _] | _] = get_slot_map(R),
    Port.

get_pod_name_from_port(Port) ->
    N = lists:last(integer_to_list(Port)),
    "redis-" ++ [N].
