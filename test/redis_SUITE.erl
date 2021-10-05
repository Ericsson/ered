-module(redis_SUITE).


%export([init_per_suite/1, end_per_suite/1]).

-compile([export_all]).

all() ->
    [t_cluster_start,
     t_command,
     t_command_all,
     t_command_client,
     t_command_pipeline,
     t_scan_delete_keys].
   %  t_hard_failover,
   %  t_manual_failover].
%     t_split_data].

init_per_suite(Config) ->
    %% TODO use port_command so we can get the exit code here?
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


t_command_pipeline(_) ->
    R = start_cluster(),
    Cmds = [[<<"SET">>, <<"{k}1">>, <<"1">>], [<<"SET">>, <<"{k}2">>, <<"2">>]],
    {ok, [<<"OK">>, <<"OK">>]} = redis:command(R, Cmds, <<"k">>),
    no_more_msgs().

t_hard_failover(_) ->
    R = start_cluster(),
    %% TODO do not hardcode port
    os:cmd("redis-cli -p 30002 DEBUG SEGFAULT"),
    [{ok, <<"PONG">>}, {error,client_stopped}, {ok, <<"PONG">>}] = redis:command_all(R, [<<"PING">>]),
    %% TODO, improve these messages, too long, maybe a map?
    {connection_status,{_Pid,{"127.0.0.1",30002},undefined}, {connection_down,{recv_error,closed}}} = get_msg(),
    {slot_map_updated, _ClusterSlotsReply} = get_msg(),
    {connection_status, _, connection_up} = get_msg(),
    {connection_status, _, fully_connected} = get_msg(),
    [{ok, <<"PONG">>}, {ok, <<"PONG">>}, {ok, <<"PONG">>}] = redis:command_all(R, [<<"PING">>]),

    %% TODO Restart 30002
    no_more_msgs().

t_manual_failover(_) ->
    R = start_cluster(),
    "OK\n" = os:cmd("redis-cli -p 30005 CLUSTER FAILOVER"),

    lists:foreach(fun(N) ->
%                          {ok, <<"OK">>} = redis:command(R, [<<"SET">>, N, N], N)
                              {ok, _} = redis:command(R, [<<"SET">>, N, N], N)
                  end,
                  [integer_to_binary(N) || N <- lists:seq(1,100)]),
    
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


    
%% scan_delete_loop(Redis, Client, _OutstandingUnlink=0, ScanDone=true) ->
%%     ok;
%% scan_delete_loop(Redis, Client, OutstandingUnlink, ScanDone) ->
%%     receive
%%         {scan, {ok, <<"0">>, Keys}} ->
%%             unlink_cmd(Redis, Client, Keys),
%%             scan_delete_loop(Redis, Client, OutstandingUnlink+1, _ScanDone=true);

%%         {scan, {ok, Cursor, Keys}} ->
%%             scan_cmd(Redis, Client, Cursor),
%%             unlink_cmd(Redis, Client, Keys),
%%             scan_delete_loop(Redis, Client, OutstandingUnlink+1, ScanDone);

%%         {unlink, {ok, _}} ->
%%             scan_delete_loop(Redis, Client, OutstandingUnlink-1, ScanDone);

%%         Other ->
%%             exit({error, Other})
%%     end.



%% scan_delete() ->
%%     scan(<<"0">>),
%%     fun Loop(<<"0">>, 0) ->
%%             ok;
%%         Loop(Apa, N) ->
%%             receive
%%                 %% {scan, {ok, <<"0">>, Keys}} ->
%%                 %%    case redis:request(Redis,  [<<"UNLINK">> | Keys]) of
%%                 %%        {ok, _} ->
%%                 %%            do_nothing;
%%                 %%        Other ->
%%                 %%            exit(error, Other)
%%                 %%    end;
%%                 {scan, {ok, <<"0">>, Keys}} ->
%%                     unlink_(Keys),
%%                     Loop(done, N+1);
%%                 {scan, {ok, Cursor, Keys}} ->
%%                     scan(Cursor),
%%                     unlink_(Keys),
%%                     Loop(not_done, N+1);
%%                 {unlink, {ok, _}} ->
%%                     Loop(Cursor, N-1);
%%                 Other ->
%%                     exit(error, Other}
%%             end
%%     end().


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
