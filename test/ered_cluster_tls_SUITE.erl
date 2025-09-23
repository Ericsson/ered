-module(ered_cluster_tls_SUITE).

-include("ered_test_utils.hrl").

-compile([export_all, nowarn_export_all]).

all() ->
    [t_command,
     {group, require_faketime}].

groups() ->
    %% Tests that require 'faketime' to manipulate the system time.
    [{require_faketime, [sequence],
      [t_expired_cert_tls_1_2,
       t_expired_cert_tls_1_3]}].

-define(PORTS, [31001, 31002, 31003, 31004, 31005, 31006]).

-define(DEFAULT_SERVER_DOCKER_IMAGE, "valkey/valkey:8.0.1").

-define(TLS_OPTS, [{cacertfile, "tls/ca.crt"},
                   {certfile,   "tls/client.crt"},
                   {keyfile,    "tls/client.key"},
                   {verify,     verify_peer},
                   {server_name_indication, "Server"}]).

-define(CLIENT_OPTS, [{connection_opts, [{tls_options, ?TLS_OPTS},
                                         {connect_timeout, 500}]}]).

init_per_suite(_Config) ->
    stop_containers(), % just in case there is junk from previous runs
    generate_tls_certs(),
    {ok, _} = application:ensure_all_started(ered, temporary),
    start_containers(),
    create_cluster(),
    ered_test_utils:wait_for_consistent_cluster(?PORTS, ?CLIENT_OPTS),
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
    case catch ered_test_utils:check_consistent_cluster(?PORTS, ?CLIENT_OPTS) of
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
    ServerBinary = case Image of
                       "redis" ++ _ -> "redis-server";
                       _Other       -> "valkey-server"
                   end,
    EnableDebugCommand = case Image of
                             "redis:" ++ [N, $. | _] when N >= $1, N < $7 ->
                                 ""; % Option does not exist.
                             _Redis7 ->
                                 " --enable-debug-command yes"
                         end,
    {ok, Path} = file:get_cwd(),
    cmd_log([io_lib:format("docker run --name redis-tls-~p -d --net=host"
                           " -v ~s/tls:/tls:ro"
                           " --restart=on-failure ~s ~s"
                           "~s"
                           " --cluster-enabled yes --tls-cluster yes"
                           " --tls-port ~p --port 0"
                           " --tls-replication yes"
                           " --tls-cert-file /tls/server.crt"
                           " --tls-key-file /tls/server.key"
                           " --tls-ca-cert-file /tls/ca.crt"
                           " --cluster-node-timeout 2000;",
                           [P, Path, Image, ServerBinary, EnableDebugCommand, P])
             || P <- ?PORTS]),

    ered_test_utils:wait_for_all_nodes_available(?PORTS, ?CLIENT_OPTS).

stop_containers() ->
    cmd_log([io_lib:format("docker stop redis-tls-~p; docker rm redis-tls-~p;", [P, P])
             || P <- ?PORTS ++ [cli]]).

create_cluster() ->
    Image = os:getenv("SERVER_DOCKER_IMAGE", ?DEFAULT_SERVER_DOCKER_IMAGE),
    Cli = case Image of
              "redis" ++ _ -> "redis-cli";
              _Other       -> "valkey-cli"
          end,
    Hosts = [io_lib:format("127.0.0.1:~p ", [P]) || P <- ?PORTS],
    {ok, Path} = file:get_cwd(),
    Cmd = io_lib:format("echo 'yes' | "
                        "docker run --name redis-tls-cli --rm --net=host -v ~s/tls:/tls:ro -i ~s "
                        "~s --tls --cacert /tls/ca.crt --cert /tls/server.crt --key /tls/server.key"
                        " --cluster-replicas 1 --cluster create ~s",
                        [Path, Image, Cli, Hosts]),
    cmd_log(Cmd).

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
    Opts = [{client_opts, ?CLIENT_OPTS}],
    R = ered_test_utils:start_cluster(?PORTS, Opts),

    lists:foreach(fun(N) ->
                          {ok, <<"OK">>} = ered_cluster:command(R, [<<"SET">>, N, N], N)
                  end,
                  [integer_to_binary(N) || N <- lists:seq(1,100)]),
    no_more_msgs().

%% Setup ered using an expired client certificate in TLSv1.2.
t_expired_cert_tls_1_2(_) ->
    generate_expired_client_cert(),

    ClientOpts = [{connection_opts, [{tls_options, ?TLS_OPTS ++ [{versions, ['tlsv1.2']}]}]}],

    {ok, _R} = ered_cluster:connect([{"127.0.0.1", 31001}],
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

    {ok, _R} = ered_cluster:connect([{"127.0.0.1", 31001}],
                                    [{info_pid, [self()]}, {client_opts, ClientOpts}]),

    ?MSG(#{msg_type := socket_closed, addr := {"127.0.0.1", 31001},
           reason := {recv_exit,
                      {tls_alert,
                       {certificate_expired, _}}}}),
    ?MSG(#{msg_type := node_down_timeout, addr := {"127.0.0.1", 31001}}, 2500),
    no_more_msgs().
