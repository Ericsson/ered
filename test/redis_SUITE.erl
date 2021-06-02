-module(redis_SUITE).


%export([init_per_suite/1, end_per_suite/1]).

-compile([export_all]).

all() ->
    [t_cluster].
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
		  [30001, 30002, 30003, 30004, 30005]),
    os:cmd(" echo 'yes' | docker run --name redis-cluster --net=host -i redis redis-cli --cluster create 127.0.0.1:30001 127.0.0.1:30002 127.0.0.1:30003 127.0.0.1:30004 127.0.0.1:30005 127.0.0.1:30006 --cluster-replicas 1"),
    [].

end_per_suite(Config) ->
    os:cmd("docker stop redis-cluster; docker rm redis-cluster;"
	   "docker stop redis-1; docker rm redis-1;"
	   "docker stop redis-2; docker rm redis-2;"
	   "docker stop redis-3; docker rm redis-3;"
	   "docker stop redis-4; docker rm redis-4;"
	   "docker stop redis-5; docker rm redis-5;"
	   "docker stop redis-6; docker rm redis-6").


t_cluster(_) ->
    redis_cluster2:start_link("127.0.0.1", 30001, []).

t_split_data(_) ->
    timer:sleep(5000),
    Data = iolist_to_binary([<<"A">> || _ <- lists:seq(0,3000)]),
    Conn1 = redis_connection:connect("127.0.0.1", 30001),
    redis_connection:request(Conn1, [<<"hello">>, <<"3">>]),
    <<"OK">> = redis_connection:request(Conn1, [<<"set">>, <<"key1">>, Data]),
    Data = redis_connection:request(Conn1, [<<"get">>, <<"key1">>]),
    ok.
