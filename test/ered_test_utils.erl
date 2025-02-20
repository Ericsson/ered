-module(ered_test_utils).

-include("ered_test_utils.hrl").

-export([start_cluster/2,
         check_consistent_cluster/2,
         wait_for_consistent_cluster/2,
         wait_for_all_nodes_available/2]).

%% Start a cluster client and wait for cluster_ok.
start_cluster(Ports, Opts) ->
    [Port1, Port2 | PortsRest] = Ports,
    InitialNodes = [{"127.0.0.1", Port} || Port <- [Port1, Port2]],

    {ok, P} = ered:connect_cluster(InitialNodes, [{info_pid, [self()]}] ++ Opts),

    ConnectedInit = [?MSG(#{msg_type := connected, addr := {"127.0.0.1", Port}})
                     || Port <- [Port1, Port2]],

    #{slot_map := SlotMap} = ?MSG(#{msg_type := slot_map_updated}, 1000),

    IdMap =  maps:from_list(lists:flatmap(
                              fun([_,_|Nodes]) ->
                                      [{Port, Id} || [_Addr, Port, Id |_]<- Nodes]
                              end, SlotMap)),

    ConnectedRest = [#{msg_type := connected} = ?MSG(#{addr := {"127.0.0.1", Port}})
                     || Port <- PortsRest],

    ClusterIds = [Id || #{cluster_id := Id} <- ConnectedInit ++ ConnectedRest],
    ClusterIds = [maps:get(Port, IdMap) || Port <- Ports],

    ?MSG(#{msg_type := cluster_ok}),

    %% Clear all old data
    [{ok, _} = ered:command_client(Client, [<<"FLUSHDB">>]) || Client <- ered:get_clients(P)],

    no_more_msgs(),
    P.

%% Check if all nodes have the same single view of the slot map and that
%% all cluster nodes are included in the slot map.
check_consistent_cluster(Ports, ClientOpts) ->
    SlotMaps = [fun(Port) ->
                        {ok, Pid} = ered_client:connect("127.0.0.1", Port, ClientOpts),
                        {ok, SlotMap} = ered_client:command(Pid, [<<"CLUSTER">>, <<"SLOTS">>]),
                        ered_client:close(Pid),
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

%% Wait until cluster is consistent, i.e all nodes have the same single view
%% of the slot map and all cluster nodes are included in the slot map.
wait_for_consistent_cluster(Ports, ClientOpts) ->
    fun Loop(N) ->
            case ered_test_utils:check_consistent_cluster(Ports, ClientOpts) of
                ok ->
                    true;
                {error, _} when N > 0 ->
                    timer:sleep(500),
                    Loop(N-1);
                {error, SlotMaps} ->
                    error({timeout_consistent_cluster, SlotMaps})
            end
    end(20).

%% Wait for all nodes to be available for communication.
wait_for_all_nodes_available(Ports, ClientOpts) ->
    Clients = [fun(Port) ->
                       {ok, Client} = ered_client:connect("127.0.0.1", Port, [{info_pid, self()}] ++ ClientOpts),
                       Client
               end(P) || P <- Ports],
    wait_for_connection_up(Clients),
    no_more_msgs().

wait_for_connection_up([]) ->
    ok;
wait_for_connection_up(Clients) ->
    #{client_id := Client} = ?MSG(#{msg_type := connected}, 15000),
    {ok, <<"PONG">>} = ered_client:command(Client, [<<"ping">>]),

    %% Stop client and allow optional connect_error events
    ered_client:close(Client),
    ?MSG(#{msg_type := client_stopped, client_id := Client}),
    ?OPTIONAL_MSG(#{msg_type := connect_error, client_id := Client}),
    ?OPTIONAL_MSG(#{msg_type := connect_error, client_id := Client}),
    wait_for_connection_up(lists:delete(Client, Clients)).

no_more_msgs() ->
    {messages,Msgs} = erlang:process_info(self(), messages),
    case  Msgs of
        [] ->
            ok;
        Msgs ->
            error({unexpected,Msgs})
    end.


