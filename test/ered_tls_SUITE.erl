-module(ered_tls_SUITE).

-compile([export_all, nowarn_export_all]).

all() ->
    [
     t_command,
     {group, require_faketime}
    ].

groups() ->
    [
     %% Tests that require 'faketime' to manipulate the system time.
     {require_faketime, [sequence],
      [
       t_expired_cert_tls_1_2,
       t_expired_cert_tls_1_3
      ]
     }].

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

-define(PORTS, [31001, 31002, 31003, 31004, 31005, 31006]).

-define(DEFAULT_SERVER_DOCKER_IMAGE, "valkey/valkey:8.0.1").

-define(TLS_OPTS, [{cacertfile, "tls/ca.crt"},
                   {certfile,   "tls/client.crt"},
                   {keyfile,    "tls/client.key"},
                   {verify,     verify_peer},
                   {server_name_indication, "Server"}]).

-define(CONNECTION_OPTS, [{connection_opts, [{tls_options, ?TLS_OPTS}]}]).

init_per_suite(_Config) ->
    stop_containers(), % just in case there is junk from previous runs
    generate_tls_certs(),
    start_containers(),
    create_cluster(),
    wait_for_consistent_cluster(),
    [].

end_per_suite(_Config) ->
    stop_containers().

init_per_group(require_faketime, _Config) ->
    case os:find_executable("faketime") of
        false ->
            {skip, "Executable faketime not found"};
        _ ->
            ok
    end.

end_per_group(require_faketime, _Config) ->
    ok.

init_per_testcase(_Testcase, Config) ->
    %% Make sure we have a valid client cert.
    generate_client_cert(),

    %% Quick check that cluster is OK; otherwise restart everything.
    case catch check_consistent_cluster(?PORTS) of
        ok ->
            [];
        _ ->
            ct:pal("Re-initialize the cluster"),
            init_per_suite(Config)
    end.

generate_tls_certs() ->
    filelib:ensure_path("tls/"),
    %% Generate CA.
    cmd_log("openssl genrsa -out tls/ca.key 4096"),
    cmd_log("openssl req -x509 -new -nodes -sha256 -key tls/ca.key -days 3650 -subj '/O=Test/CN=Certificate Authority' -out tls/ca.crt"),
    %% Generate server certificate.
    cmd_log("openssl genrsa -out tls/server.key 2048"),
    cmd_log("openssl req -new -sha256 -key tls/server.key -subj '/O=Test/CN=Server' | "
            "openssl x509 -req -sha256 -CA tls/ca.crt -CAkey tls/ca.key -CAserial tls/ca.txt -CAcreateserial -days 1 -out tls/server.crt"),
    %% Generate client key and cert.
    cmd_log("openssl genrsa -out tls/client.key 2048"),
    generate_client_cert(),
    %% Let the pods read the key files
    cmd_log("chmod 644 tls/*.key").

generate_client_cert() ->
    cmd_log("openssl req -new -sha256 -key tls/client.key -subj '/O=Test/CN=Client' | "
            "openssl x509 -req -sha256 -CA tls/ca.crt -CAkey tls/ca.key -CAserial tls/ca.txt -CAcreateserial -days 1 -out tls/client.crt"),
    %% Since OTP caches the content of client.crt we need to clear the cache.
    ssl:clear_pem_cache().

generate_expired_client_cert() ->
    cmd_log("openssl req -new -sha256 -key tls/client.key -subj '/O=Test/CN=Client' | "
            "faketime '2020-01-01 10:00:00' "
            "openssl x509 -req -sha256 -CA tls/ca.crt -CAkey tls/ca.key -CAserial tls/ca.txt -CAcreateserial -days 1 -out tls/client.crt"),
    %% Since OTP caches the content of client.crt we need to clear the cache.
    ssl:clear_pem_cache().

start_containers() ->
    Image = os:getenv("SERVER_DOCKER_IMAGE", ?DEFAULT_SERVER_DOCKER_IMAGE),
    EnableDebugCommand = case Image of
                             "redis:" ++ [N, $. | _] when N >= $1, N < $7 ->
                                 ""; % Option does not exist.
                             _Redis7 ->
                                 " --enable-debug-command yes"
                         end,
    {ok, Path} = file:get_cwd(),
    cmd_log([io_lib:format("docker run --name redis-tls-~p -d --net=host"
                           " -v ~s/tls:/tls:ro"
                           " --restart=on-failure ~s redis-server"
                           "~s"
                           " --cluster-enabled yes --tls-cluster yes"
                           " --tls-port ~p --port 0"
                           " --tls-replication yes"
                           " --tls-cert-file /tls/server.crt"
                           " --tls-key-file /tls/server.key"
                           " --tls-ca-cert-file /tls/ca.crt"
                           " --cluster-node-timeout 2000;",
                           [P, Path, Image, EnableDebugCommand, P])
             || P <- ?PORTS]),

    timer:sleep(3000),
    lists:foreach(fun(Port) ->
                          {ok,Pid} = ered_client:start_link("127.0.0.1", Port, ?CONNECTION_OPTS),
                          {ok, <<"PONG">>} = ered_client:command(Pid, [<<"ping">>]),
                          ered_client:stop(Pid)
                  end, ?PORTS).

stop_containers() ->
    cmd_log([io_lib:format("docker stop redis-tls-~p; docker rm redis-tls-~p;", [P, P])
             || P <- ?PORTS ++ [cli]]).

create_cluster() ->
    Image = os:getenv("SERVER_DOCKER_IMAGE", ?DEFAULT_SERVER_DOCKER_IMAGE),
    Hosts = [io_lib:format("127.0.0.1:~p ", [P]) || P <- ?PORTS],
    {ok, Path} = file:get_cwd(),
    Cmd = io_lib:format("echo 'yes' | "
                        "docker run --name redis-tls-cli --rm --net=host -v ~s/tls:/tls:ro -i ~s "
                        "redis-cli --tls --cacert /tls/ca.crt --cert /tls/server.crt --key /tls/server.key"
                        " --cluster-replicas 1 --cluster create ~s",
                        [Path, Image, Hosts]),
    cmd_log(Cmd).

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
                        {ok,Pid} = ered_client:start_link("127.0.0.1", Port, ?CONNECTION_OPTS),
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

start_cluster() ->
    [Port1, Port2 | PortsRest] = Ports = ?PORTS,
    InitialNodes = [{"127.0.0.1", Port} || Port <- [Port1, Port2]],

    %% wait_for_consistent_cluster(),
    {ok, P} = ered:start_link(InitialNodes, [{info_pid, [self()]}, {client_opts, ?CONNECTION_OPTS}]),

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

no_more_msgs() ->
    {messages,Msgs} = erlang:process_info(self(), messages),
    case  Msgs of
        [] ->
            ok;
        Msgs ->
            error({unexpected,Msgs})
    end.

cmd_log(Cmd) ->
    R = os:cmd(Cmd),
    ct:pal("~s\n~s\n", [Cmd, R]),
    R.


%% Basic test of commands when using TLS.
t_command(_) ->
    R = start_cluster(),
    lists:foreach(fun(N) ->
                          {ok, <<"OK">>} = ered:command(R, [<<"SET">>, N, N], N)
                  end,
                  [integer_to_binary(N) || N <- lists:seq(1,100)]),
    no_more_msgs().

%% Setup ered using an expired client certificate in TLSv1.2.
t_expired_cert_tls_1_2(_) ->
    generate_expired_client_cert(),

    ClientOpts = [{connection_opts, [{tls_options, ?TLS_OPTS ++ [{versions, ['tlsv1.2']}]}]}],

    {ok, _R} = ered:start_link([{"127.0.0.1", 31001}],
                               [{info_pid, [self()]}, {client_opts, ClientOpts}]),

    ?MSG(#{msg_type := connect_error, addr := {"127.0.0.1", 31001},
           reason := {tls_alert,
                      {certificate_expired, _}}}),
    ?MSG(#{msg_type := node_down_timeout, addr := {"127.0.0.1", 31001}}, 2500),
    no_more_msgs().

%% Setup ered using an expired client certificate in TLSv1.3.
t_expired_cert_tls_1_3(_) ->
    generate_expired_client_cert(),

    ClientOpts = [{connection_opts, [{tls_options, ?TLS_OPTS ++ [{versions, ['tlsv1.3']}]}]}],

    {ok, _R} = ered:start_link([{"127.0.0.1", 31001}],
                               [{info_pid, [self()]}, {client_opts, ClientOpts}]),

    ?MSG(#{msg_type := socket_closed, addr := {"127.0.0.1", 31001},
           reason := {recv_exit,
                      {tls_alert,
                       {certificate_expired, _}}}}),
    ?MSG(#{msg_type := node_down_timeout, addr := {"127.0.0.1", 31001}}, 2500),
    no_more_msgs().
