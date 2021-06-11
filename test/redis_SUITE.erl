-module(redis_SUITE).


%export([init_per_suite/1, end_per_suite/1]).

-compile([export_all]).

all() ->
    [t_cluster_start,
     t_command,
     t_command_all,
     t_command_client].
%     t_split_data].

init_per_suite(Config) ->
    os:cmd("docker run --name redis-1 -d --net=host redis redis-server --cluster-enabled yes --port 30001;"
	   "docker run --name redis-2 -d --net=host redis redis-server --cluster-enabled yes --port 30002;"
	   "docker run --name redis-3 -d --net=host redis redis-server --cluster-enabled yes --port 30003;"
	   "docker run --name redis-4 -d --net=host redis redis-server --cluster-enabled yes --port 30004;"
	   "docker run --name redis-5 -d --net=host redis redis-server --cluster-enabled yes --port 30005;"
	   "docker run --name redis-6 -d --net=host redis redis-server --cluster-enabled yes --port 30006;"),
    timer:sleep(1000),
    lists:foreach(fun(Port) ->
			  {ok,Pid} = redis_client:start_link("127.0.0.1", Port, []),
			  {ok, <<"PONG">>} = redis_client:request(Pid, <<"ping">>)
		  end,
		  [30001, 30002, 30003, 30004, 30005, 30006]),
    os:cmd(" echo 'yes' | docker run --name redis-cluster --net=host -i redis redis-cli --cluster create 127.0.0.1:30001 127.0.0.1:30002 127.0.0.1:30003 127.0.0.1:30004 127.0.0.1:30005 127.0.0.1:30006 --cluster-replicas 1"),

    %% Wait for cluster to be fully up. If not then we get a "CLUSTERDOWN The cluster is down" error when sendong commands.
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


t_cluster_start(_) ->
    %% io:format("hek", []),
    %% R = os:cmd("redis-cli -p 30001 cluster slots"),
    %% apa = R,
    %% ct:log(info, "~w", [R]),
    Pid = self(),
%    receive apa -> apa after 5000 -> ok end,
    {ok, P} = redis:start_link(localhost, 30001, [{info_cb, fun(Msg) -> Pid ! Msg end}]),
%    {ok, P} = redis_cluster2:start_link(localhost, 30001, [{info_cb, fun(Msg) -> Pid ! Msg end}]),

    {connection_status, _, connection_up} = get_msg(),
    {slot_map_updated, ClusterSlotsReply} = get_msg(),
    ct:pal("~p\n", [ClusterSlotsReply]),
    {connection_status, _, connection_up} = get_msg(),
    {connection_status, _, connection_up} = get_msg(),
    {connection_status, _, connection_up} = get_msg(),

    {connection_status, _, fully_connected} = get_msg(),
    no_more_msgs(),
    ok.
    %{connection_status, _, connection_down} = get_msg(),
    %% Kill a master
    %os:cmd("redis-cli -p 30002 DEBUG SEGFAULT"),
   
    %{connection_status, _, connection_down} = get_msg(60000),

    %receive apa -> apa after 60000 -> throw(tc_timeout) end.

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

        

    
%% TEST blocked master, slot update other node
%% TEST connect no redis instance
%% TEST cluster move
%% TEST incomplete map connection status
%% TEST pipeline
%% TEST manual failover


t_split_data(_) ->
    timer:sleep(5000),
    Data = iolist_to_binary([<<"A">> || _ <- lists:seq(0,3000)]),
    Conn1 = redis_connection:connect("127.0.0.1", 30001),
    redis_connection:request(Conn1, [<<"hello">>, <<"3">>]),
    <<"OK">> = redis_connection:request(Conn1, [<<"set">>, <<"key1">>, Data]),
    Data = redis_connection:request(Conn1, [<<"get">>, <<"key1">>]),
    ok.




start_cluster() ->
    Pid = self(),
    {ok, P} = redis:start_link(localhost, 30001, [{info_cb, fun(Msg) -> Pid ! Msg end}]),
    {connection_status, _, connection_up} = get_msg(),
    {slot_map_updated, _ClusterSlotsReply} = get_msg(),
    {connection_status, _, connection_up} = get_msg(),
    {connection_status, _, connection_up} = get_msg(),
    {connection_status, _, connection_up} = get_msg(),

    {connection_status, _, fully_connected} = get_msg(),
    no_more_msgs(),
    P.

get_msg() ->
    get_msg(1000).

get_msg(Timeout) ->
    receive Msg -> Msg after Timeout -> timeout end.

no_more_msgs() ->
    timeout = get_msg(0).

scan_helper(Client, Curs0, Acc0) ->
    {ok, [Curs1, Keys]} = redis:command_client(Client, [<<"SCAN">>, Curs0]),
    Acc1 = [Keys|Acc0],
    case Curs1 of
        <<"0">> ->
            Acc1;
        _ ->
            scan_helper(Client, Curs1, Acc1)
    end.
