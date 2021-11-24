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
     t_split_data].


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
			  {ok, <<"PONG">>} = redis_client:request(Pid, <<"ping">>)
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
	   "docker stop redis-6; docker rm redis-6").


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

%% t_hard_failover(_) ->
%%     R = start_cluster(),
%%     ct:pal("~p\n", [redis:command_all(R, [<<"CLUSTER">>, <<"SLOTS">>])]),
%%     %% TODO do not hardcode port
%%     ct:pal(os:cmd("docker ps")),
%%     ct:pal(os:cmd("docker stop redis-2")),
%%     timer:sleep(5000),
%%     ct:pal("~p\n", [redis:command_all(R, [<<"CLUSTER">>, <<"SLOTS">>])]),
    
%%     ct:pal(os:cmd("docker ps")),
%%     ct:pal(os:cmd("docker start redis-2")),
%%     ct:pal(os:cmd("docker ps")),
%%     timer:sleep(5000),
        
%% %    {connection_status,{_Pid,{"127.0.0.1",30002},undefined}, {connection_down,{recv_exit,closed}}} = get_msg(10000),
%%     ct:pal(os:cmd("docker ps")),
%%     ct:pal("~p\n", [redis:command_all(R, [<<"CLUSTER">>, <<"SLOTS">>])]).

t_hard_failover(_) ->
    R = start_cluster(),
    Port = get_master_port(R),
    Pod = get_pod_name_from_port(Port),
    ct:pal("~p\n", [redis:command_all(R, [<<"CLUSTER">>, <<"SLOTS">>])]),
    ct:pal(os:cmd("docker stop " ++ Pod)),

    connection_down([{localhost, Port}, {"127.0.0.1", Port}], {recv_exit,closed}),

    {slot_map_updated, _ClusterSlotsReply} = get_msg(5000),

    %% connection not in slotmap
    connection_down([{"127.0.0.1", Port}], {client_stopped,normal}),


    ct:pal("~p\n", [redis:command_all(R, [<<"CLUSTER">>, <<"SLOTS">>])]),

    %ct:pal(os:cmd("docker ps")),
    ct:pal(os:cmd("docker start " ++ Pod)),
    %ct:pal(os:cmd("docker ps")),
    %timer:sleep(5000),
%    {connection_status,{_Pid,{"127.0.0.1",30002},undefined}, {connection_down,{recv_exit,closed}}} = get_msg(10000),
    %ct:pal(os:cmd("docker ps")),
    %ct:pal("~p\n", [redis:command_all(R, [<<"CLUSTER">>, <<"SLOTS">>])]),

    %% node back
    connection_up([{localhost, Port}], 10000),

    {slot_map_updated, _} = get_msg(10000),

    connection_up([{"127.0.0.1", Port}]),

    {connection_status, _, fully_connected} = get_msg(3000),
    no_more_msgs().



get_master_port(R) ->
    [Client|_] = redis:get_clients(R),
    {ok, SlotMap} = redis:command_client(Client, [<<"CLUSTER">>, <<"SLOTS">>]),
    [[_SlotStart, _SlotEnd, [_Ip, Port |_] | _] | _] = SlotMap,
    Port.

get_pod_name_from_port(Port) ->
    N = lists:last(integer_to_list(Port)),
    "redis-" ++ [N].

    %ct:pal("~p\n", [redis:command_all(R, [<<"CLUSTER">>, <<"SLOTS">>])]),
    %% [{ok, <<"PONG">>}, {error,{client_stopped, normal}}, {ok, <<"PONG">>}] = redis:command_all(R, [<<"PING">>]),
    %% %% TODO, improve these messages, too long, maybe a map?
    %% {connection_status,{_Pid,{"127.0.0.1",30002},undefined}, {connection_down,{recv_exit,closed}}} = get_msg(),
    %% {slot_map_updated, _ClusterSlotsReply} = get_msg(),
    %% {connection_status, _, fully_connected} = get_msg(),

    
    %% [{ok, <<"PONG">>}, {ok, <<"PONG">>}, {ok, <<"PONG">>}] = redis:command_all(R, [<<"PING">>]),
    %% timer:sleep(5000),
    %% SlotMaps = redis:command_all(R, [<<"CLUSTER">>, <<"SLOTS">>]),
    %% ct:pal("~p\n", [SlotMaps]),
    %% ct:pal(os:cmd("docker ps")),
    %% {connection_status, _, connection_up} = get_msg(10000),
    %% %% TODO Restart 30002
    %% no_more_msgs().


%% t_hard_failover(_) ->
%%     R = start_cluster(),
%%     %% TODO do not hardcode port
%%     ct:pal(os:cmd("docker ps")),
%%     os:cmd("redis-cli -p 30002 DEBUG SEGFAULT"),
%%     {connection_status,{_Pid,{"127.0.0.1",30002},undefined}, {connection_down,{recv_exit,closed}}} = get_msg(10000),
%%     ct:pal("~p\n", [redis:command_all(R, [<<"CLUSTER">>, <<"SLOTS">>])]),

%%     timer:sleep(1000),
%%     ct:pal("~p\n", [redis:command_all(R, [<<"CLUSTER">>, <<"SLOTS">>])]),
%%     [{ok, <<"PONG">>}, {error,{client_stopped, normal}}, {ok, <<"PONG">>}] = redis:command_all(R, [<<"PING">>]),
%%     %% TODO, improve these messages, too long, maybe a map?
%%     {connection_status,{_Pid,{"127.0.0.1",30002},undefined}, {connection_down,{recv_exit,closed}}} = get_msg(),
%%     {slot_map_updated, _ClusterSlotsReply} = get_msg(),
%%     {connection_status, _, fully_connected} = get_msg(),

%%     [{ok, <<"PONG">>}, {ok, <<"PONG">>}, {ok, <<"PONG">>}] = redis:command_all(R, [<<"PING">>]),
%%     timer:sleep(5000),
%%     SlotMaps = redis:command_all(R, [<<"CLUSTER">>, <<"SLOTS">>]),
%%     ct:pal("~p\n", [SlotMaps]),
%%     ct:pal(os:cmd("docker ps")),
%%     {connection_status, _, connection_up} = get_msg(10000),
%%     %% TODO Restart 30002
%%     no_more_msgs().

t_manual_failover(_) ->
    R = start_cluster(),
    %% "OK\n" = os:cmd("redis-cli -p 30005 CLUSTER FAILOVER"),
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
    {slot_map_updated, _ClusterSlotsReply} = get_msg().
%% TODO no_more_msgs



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
    redis:command_client_cb(Client, [<<"SCAN">>, Cursor, <<"COUNT">>, integer_to_binary(100), <<"MATCH">>, <<"otherkey*">>], Callback).

unlink_cmd(Client, []) ->
    0;
unlink_cmd(Client, Keys) ->
    Pid = self(),
    Callback = fun(Response) -> Pid ! {unlink, Response} end,
    redis:command_client_cb(Client, [[<<"UNLINK">>, Key] || Key <- Keys], Callback),
    1.

group_by_hash(Keys) ->
    lists:foldl(fun(Key, Acc) ->
                        Fun = fun(KeysForSlot) -> [Key|KeysForSlot] end,
                        maps:update_with(redis_lib:hash(Key), Fun, _Init = [], Acc)
                end,
                #{},
                Keys).
 


%% 6> inet_db:add_host({127,0,0,2}, [apa]).  

%% 33> inet_db:set_lookup([file]). 
%% ok
%% 34> inet_tcp:getaddrs(apa).                    
%% {ok,[{127,0,0,2}]}
%% 35> 



%% TEST blocked master, slot update other node
%% TEST connect no redis instance
%% TEST cluster move
%% TEST incomplete map connection status
%% TEST pipeline
%% TEST non-contiguous slots
%% TEST TCP close from Redis, no alarm
%% TEST packets dropped after reconnect timeout
%% TEST DNS map to invalid addr, try next one

%% Test to send and receive a big data packet. The read should be split
t_split_data(_) ->
    Data = iolist_to_binary([<<"A">> || _ <- lists:seq(0,3000)]),
    R = start_cluster(),
    {ok, <<"OK">>} = redis:command(R, [<<"SET">>, <<"key1">>, Data], <<"key1">>),
    {ok, Data} = redis:command(R, [<<"GET">>, <<"key1">>], <<"key1">>),
    ok.




start_cluster() ->
    Pid = self(),

    Ports = [30001, 30002, 30003, 30004, 30005, 30006],
    InitialNodes = [{localhost, Port} || Port <- Ports],

    {ok, P} = redis:start_link(InitialNodes, [{info_cb, fun(Msg) -> Pid ! Msg end}]),
    %% {connection_status, _, connection_up} = get_msg(),


    %% [receive {connection_status, {_Pid, Addr, _Id},  connection_up} -> ok after 1000 -> exit(timeout) end
    %%  || Addr <- InitialNodes

    connection_up(InitialNodes),
    {slot_map_updated, _ClusterSlotsReply} = get_msg(),
    connection_up([{"127.0.0.1", Port} || Port <- Ports]),
    


    %% ok = receive {connection_status, _, connection_up} -> ok after 1000 -> timeout end,
    %% ok = receive {connection_status, _, connection_up} -> ok after 1000 -> timeout end,
    %% ok = receive {connection_status, _, connection_up} -> ok after 1000 -> timeout end,
    %% ok = receive {connection_status, _, connection_up} -> ok after 1000 -> timeout end,
    %% ok = receive {connection_status, _, connection_up} -> ok after 1000 -> timeout end,
    %% ok = receive {connection_status, _, connection_up} -> ok after 1000 -> timeout end,

    {connection_status, _, fully_connected} = get_msg(),
    no_more_msgs(),
    P.

connection_up(Addrs) ->
    connection_up(Addrs, 1000).

connection_up(Addrs, Timeout) ->
    [receive
         {connection_status, {_Pid, Addr, _Id},  connection_up} -> ok
     after
         Timeout -> error({timeout, Addr})
     end
     || Addr <- Addrs].

connection_down(Addrs, Reason) ->
    connection_down(Addrs, Reason, 1000).

connection_down(Addrs, Reason, Timeout) ->
    [receive
         {connection_status, {_Pid, Addr, _Id}, {connection_down, Reason}} ->
             ok
     after
         Timeout -> error({timeout, Addr})
     end
     || Addr <- Addrs].



get_msg() ->
    get_msg(1000).

get_msg(Timeout) ->
    receive Msg -> Msg after Timeout -> get_msg_timeout end.

no_more_msgs() ->
    get_msg_timeout = get_msg(0).

scan_helper(Client, Curs0, Acc0) ->
    {ok, [Curs1, Keys]} = redis:command_client(Client, [<<"SCAN">>, Curs0]),
    Acc1 = [Keys|Acc0],
    case Curs1 of
        <<"0">> ->
            Acc1;
        _ ->
            scan_helper(Client, Curs1, Acc1)
    end.
