-module(ered).

%% External API for using connecting and sending commands.

%% API
-export([connect_cluster/2, connect_client/3,
         close/1,
         command/2, command/3, command/4,
         command_async/3, command_async/4,
         command_all/2, command_all/3,
         command_client/2, command_client/3,    % deprecated
         command_client_async/3,                % deprecated
         get_clients/1,
         get_addr_to_client_map/1,
         update_slots/1, update_slots/2]).

-export_type([client_opt/0,
              cluster_opt/0,
              command/0,
              reply/0,
              reply_fun/0,
              client_ref/0,
              cluster_ref/0]).

%%%===================================================================
%%% Definitions
%%%===================================================================

-type cluster_opt() :: ered_cluster:opt().
-type client_opt()  :: ered_client:opt().
-type addr()        :: {host(), inet:port_number()}.
-type host()        :: inet:socket_address() | inet:hostname().
-type command()     :: ered_command:command().
-type reply()       :: ered_client:reply() | {error, unmapped_slot | client_down}.
-type reply_fun()   :: ered_client:reply_fun().
-type key()         :: binary().
-type ered_ref()    :: cluster_ref() | client_ref().
-type cluster_ref() :: {cluster, pid()}.
-type client_ref()  :: {client, pid()}.

%%%===================================================================
%%% API
%%%===================================================================

%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-spec connect_cluster([addr()], [cluster_opt()]) -> {ok, cluster_ref()} | {error, term()}.
%%
%% Connect to a cluster. A cluster handling process manages sets up
%% client connections to the provided addresses and fetches the
%% cluster slot map. Once there is a complete slot map and all node
%% are connected, the cluster client is ready to serve requests.
%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
connect_cluster(Addrs, Opts) ->
    try ered_cluster_sup:start_child() of
        {ok, ClusterSup} ->
            {ok, ClientSup} = ered_dyn_cluster_sup:start_client_sup(ClusterSup),
            {ok, ClusterPid} = ered_dyn_cluster_sup:start_cluster_mgr(ClusterSup, Addrs, Opts, ClientSup, self()),
            {ok, {cluster, ClusterPid}}
    catch exit:{noproc, _} ->
            {error, ered_not_started}
    end.

%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-spec connect_client(host(), inet:port_number(), [client_opt()]) -> {ok, client_ref()} | {error, term()}.
%%
%% Open a single client connection to a node.
%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
connect_client(Host, Port, Opts) ->
    case ered_client:connect(Host, Port, Opts) of
        {ok, Pid} ->
            {ok, {client, Pid}};
        Error ->
            Error
    end.

%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-spec close(ered_ref()) -> ok.
%%
%% Closes the connection(s).
%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
close({cluster, Pid}) ->
    ered_cluster:stop(Pid);
close({client, Pid}) ->
    ered_client:close(Pid).

%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-spec command(ered_ref(), command()) -> reply().
-spec command(ered_ref(), command(), key()) -> reply();
             (ered_ref(), command(), timeout()) -> reply().
-spec command(ered_ref(), command(), key(), timeout()) -> reply().
%%
%% Send a command. In cluster mode, the command will be routed to
%% the correct node based on the provided key.
%% If the command is a single command then it is represented as a
%% list of binaries where the first binary is the command name
%% to execute and the rest of the binaries are the arguments.
%% If the command is a pipeline, e.g. multiple commands to executed
%% then they need to all map to the same slot for things to
%% work as expected.
%% For cluster clients, a key must be provided.
%% Omitting timeout is the same as setting the timeout to infinity.
%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
command({client, Pid}, Command) ->
    ered_client:command(Pid, Command, infinity).
command({cluster, Pid}, Command, Key) when is_binary(Key) ->
    ered_cluster:command(Pid, Command, Key, infinity);
command({client, Pid}, Command, Key) when is_binary(Key)  ->
    ered_client:command(Pid, Command, infinity);
command({client, Pid}, Command, Timeout) when is_integer(Timeout); Timeout =:= infinity ->
    ered_client:command(Pid, Command, Timeout).
command({cluster, Pid}, Command, Key, Timeout) ->
    ered_cluster:command(Pid, Command, Key, Timeout);
command({client, Pid}, Command, _Key, Timeout) ->
    ered_client:command(Pid, Command, Timeout).

%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-spec command_async(ered_ref(), command(), fun((reply()) -> any())) -> ok.
-spec command_async(ered_ref(), command(), key(), fun((reply()) -> any())) -> ok.
%%
%% Like command/2,3,4 but asynchronous. Instead of returning the reply,
%% the reply function is applied to the reply when it is available.
%% The reply function runs in an unspecified process and should not
%% hang or perform any lengthy task.
%% For cluster clients, a key must be provided.
%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
command_async({client, Pid}, Command, ReplyFun) when is_function(ReplyFun, 1) ->
    ered_client:command_async(Pid, Command, ReplyFun).
command_async({cluster, Pid}, Command, Key, ReplyFun) when is_function(ReplyFun, 1) ->
    ered_cluster:command_async(Pid, Command, Key, ReplyFun);
command_async({client, Pid}, Command, _Key, ReplyFun) when is_function(ReplyFun, 1) ->
    ered_client:command_async(Pid, Command, ReplyFun).

%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-spec command_all(cluster_ref(), command()) -> [reply()].
-spec command_all(cluster_ref(), command(), timeout()) -> [reply()].
%%
%% Send the same command to all primary nodes in the cluster.
%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
command_all({cluster, Pid}, Command) ->
    ered_cluster:command_all(Pid, Command, infinity).
command_all({cluster, Pid}, Command, Timeout) ->
    [ered_client:command(Pid, Command, Timeout)].

%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-spec command_client(client_ref(), command()) -> reply().
-spec command_client(client_ref(), command(), timeout()) -> reply().
%%
%% Deprecated. Use command/2,3 instead.
%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
command_client({client, Pid}, Command) ->
    ered_client:command(Pid, Command, infinity).

command_client({client, Pid}, Command, Timeout) ->
    ered_client:command(Pid, Command, Timeout).

%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-spec command_client_async(client_ref(), command(), reply_fun()) -> ok.
%%
%% Deprecated. Use command_async/3 instead.
%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
command_client_async({client, Pid}, Command, ReplyFun) ->
    ered_client:command_async(Pid, Command, ReplyFun).

%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-spec get_clients(cluster_ref()) -> [client_ref()].
%%
%% Get clients to each of the primary nodes in a cluster
%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
get_clients({cluster, ClusterPid}) ->
    [{client, Pid} || Pid <- ered_cluster:get_clients(ClusterPid)].

%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-spec get_addr_to_client_map(cluster_ref()) -> #{addr() => client_ref()}.
%%
%% Get the address to client mapping. This includes all clients.
%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
get_addr_to_client_map({cluster, ClusterPid}) ->
    AddrToPid = ered_cluster:get_addr_to_client_map(ClusterPid),
    maps:map(fun (_Addr, Pid) -> {client, Pid} end, AddrToPid).

%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-spec update_slots(cluster_ref()) -> ok.
%%
%% Trigger a slot-to-node mapping update using any connected client.
%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
update_slots({cluster, ClusterPid}) ->
    ered_cluster:update_slots(ClusterPid, any, any).

-spec update_slots(cluster_ref(), client_ref()) -> ok.
%%
%% Trigger a slot-to-node mapping update using the specified client,
%% if it's already connected. Otherwise another connected client is
%% used.
%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
update_slots({cluster, ClusterPid}, {client, ClientPid}) ->
    ered_cluster:update_slots(ClusterPid, any, ClientPid).
