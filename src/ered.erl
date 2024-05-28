-module(ered).

%% External API for using connecting and sending commands to Redis cluster.

%% API
-export([connect_cluster/2, connect_client/3,
         close/1,
         command/3, command/4,
         command_async/4,
         command_all/2, command_all/3,
         command_client/2, command_client/3,
         command_client_async/3,
         get_clients/1,
         get_addr_to_client_map/1,
         update_slots/1, update_slots/2]).

-export_type([opt/0,
              command/0,
              reply/0,
              reply_fun/0,
              client_ref/0,
              server_ref/0]).

%%%===================================================================
%%% Definitions
%%%===================================================================

-type opt()        :: ered_cluster:opt().
-type addr()       :: ered_cluster:addr().
-type host()       :: ered_connection:host().
-type server_ref() :: pid().
-type command()    :: ered_command:command().
-type reply()      :: ered_client:reply() | {error, unmapped_slot | client_down}.
-type reply_fun()  :: ered_client:reply_fun().
-type key()        :: binary().
-type client_ref() :: ered_client:server_ref().

%%%===================================================================
%%% API
%%%===================================================================

%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-spec connect_cluster([addr()], [opt()]) -> {ok, server_ref()} | {error, term()}.
%%
%% Start the cluster handling
%% process which will set up clients to the provided addresses and
%% fetch the cluster slot map. Once there is a complete slot map and
%% all Redis node clients are connected this process is ready to
%% serve requests.
%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
connect_cluster(Addrs, Opts) ->
    try ered_cluster_sup:start_child() of
        {ok, ClusterSup} ->
            {ok, ClientSup} = ered_dyn_cluster_sup:start_client_sup(ClusterSup),
            {ok, ClusterPid} = ered_dyn_cluster_sup:start_cluster_mgr(ClusterSup, Addrs, Opts, ClientSup, self()),
            {ok, ClusterPid}
    catch exit:{noproc, _} ->
            {error, ered_not_started}
    end.

%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-spec connect_client(host(), inet:port_number(), [opt()]) -> {ok, client_ref()} | {error, term()}.
%%
%% Open a single client connection to a Redis node.
%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
connect_client(Host, Port, Opts) ->
    ered_client:connect(Host, Port, Opts).

%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-spec close(server_ref()) -> ok.
%%
%% Stop the cluster handling
%% process and in turn disconnect and stop all clients.
%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
close(ServerRef) ->
    ered_cluster:stop(ServerRef).

%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-spec command(server_ref(), command(), key()) -> reply().
-spec command(server_ref(), command(), key(), timeout()) -> reply().
%%
%% Send a command to the Redis cluster. The command will be routed to
%% the correct Redis node client based on the provided key.
%% If the command is a single command then it is represented as a
%% list of binaries where the first binary is the Redis command
%% to execute and the rest of the binaries are the arguments.
%% If the command is a pipeline, e.g. multiple commands to executed
%% then they need to all map to the same slot for things to
%% work as expected.
%% Command/3 is the same as setting the timeout to infinity.
%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
command(ServerRef, Command, Key) ->
    ered_cluster:command(ServerRef, Command, Key, infinity).

command(ServerRef, Command, Key, Timeout) ->
    ered_cluster:command(ServerRef, Command, Key, Timeout).

%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-spec command_async(server_ref(), command(), key(), fun((reply()) -> any())) -> ok.
%%
%% Like command/3,4 but asynchronous. Instead of returning the reply, the reply
%% function is applied to the reply when it is available. The reply function
%% runs in an unspecified process.
%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
command_async(ServerRef, Command, Key, ReplyFun) when is_function(ReplyFun, 1) ->
    ered_cluster:command_async(ServerRef, Command, Key, ReplyFun).

%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-spec command_all(server_ref(), command()) -> [reply()].
-spec command_all(server_ref(), command(), timeout()) -> [reply()].
%%
%% Send the same command to all connected master Redis nodes.
%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
command_all(ServerRef, Command) ->
    ered_cluster:command_all(ServerRef, Command, infinity).

command_all(ServerRef, Command, Timeout) ->
    ered_cluster:command_all(ServerRef, Command, Timeout).

%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-spec command_client(client_ref(), command()) -> reply().
-spec command_client(client_ref(), command(), timeout()) -> reply().
%%
%% Send the command to a specific Redis client without any client routing.
%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
command_client(ClientRef, Command) ->
    ered_client:command(ClientRef, Command, infinity).

command_client(ClientRef, Command, Timeout) ->
    ered_client:command(ClientRef, Command, Timeout).

%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-spec command_client_async(client_ref(), command(), reply_fun()) -> ok.
%%
%% Send command to a specific Redis client in asynchronous fashion. The
%% provided callback function will be called with the reply. Note that
%% the callback function will executing in the redis client process and
%% should not hang or perform any lengthy task.
%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
command_client_async(ClientRef, Command, CallbackFun) ->
    ered_client:command_async(ClientRef, Command, CallbackFun).

%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-spec get_clients(server_ref()) -> [client_ref()].
%%
%% Get all Redis master node clients
%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
get_clients(ServerRef) ->
    ered_cluster:get_clients(ServerRef).

%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-spec get_addr_to_client_map(server_ref()) -> #{addr() => client_ref()}.
%%
%% Get the address to client mapping. This includes all clients.
%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
get_addr_to_client_map(ServerRef) ->
    ered_cluster:get_addr_to_client_map(ServerRef).

%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-spec update_slots(server_ref()) -> ok.
%%
%% Trigger a slot-to-node mapping update using any connected client.
%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
update_slots(ServerRef) ->
    ered_cluster:update_slots(ServerRef, any, any).

-spec update_slots(server_ref(), client_ref()) -> ok.
%%
%% Trigger a slot-to-node mapping update using the specified client,
%% if it's already connected. Otherwise another connected client is
%% used.
%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
update_slots(ServerRef, ClientRef) ->
    ered_cluster:update_slots(ServerRef, any, ClientRef).
