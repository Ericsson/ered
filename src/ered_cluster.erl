-module(ered_cluster).

%% Cluster handling module. Keeps track of clients and keeps the slot map
%% up to date. Cluster status can be monitored by subscribing to info
%% messages.


-behaviour(gen_server).

%% API
-export([start_link/2,
         stop/1,
         update_slots/3,
         get_slot_map_info/1,
         connect_node/2
        ]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3, format_status/2]).


-export_type([opt/0,
              addr/0]).

%%%===================================================================
%%% Definitions
%%%===================================================================

-record(st, {
             cluster_state = nok :: ok | nok,
             %% The initial configured nodes will be prioritized when
             %% fetching new slot maps and clients to these will not
             %% be closed even if they are not part of the current
             %% slot map
             initial_nodes = [] :: [addr()],
             %% Mapping from address to client for all known clients
             nodes = #{} :: #{addr() => pid()},
             %% Clients in connected state
             up = new_set([]) :: addr_set(),
             %% Clients that are currently masters
             masters = new_set([]) :: addr_set(),
             %% Clients with a full queue
             queue_full = new_set([]) :: addr_set(),
             %% Clients started but not connected yet
             pending = new_set([]) :: addr_set(),
             %% Clients that lost connection and trying to reconnect, probably a
             %% harmless situation. These are still considered 'up'.
             reconnecting = new_set([]) :: addr_set(),
             %% Clients pending to be closed. Mapped to the closing timer
             %% reference
             closing = #{} :: #{addr() => reference()},

             slot_map = [],
             slot_map_version = 0,
             slot_timer_ref = none,

             info_pid = [] :: [pid()],
             update_delay = 1000, % 1s delay between slot map update requests
             client_opts = [],
             update_slot_wait = 500,
             min_replicas = 1,
             close_wait = 10000

            }).


-type addr() :: ered_client:addr().
-type addr_set() :: sets:set(addr()).
-type server_ref() :: pid().
-type client_ref() :: ered_client:server_ref().

-type opt() ::
        %% List of pids to receive cluster info messages. See ered_info_msg module.
        {info_pid, [pid()]} |
        %% CLUSTER SLOTS command is used to fetch slots from the Redis cluster.
        %% This value sets how long to wait before trying to send the command again.
        {update_slot_wait, non_neg_integer()} |
        %% Options passed to the client
        {client_opts, [ered_client:opt()]} |
        %% For each Redis master node, the min number of replicas for the cluster
        %% to be considered OK.
        {min_replicas, non_neg_integer()} |
        %% How long to delay the closing of clients that are no longer part of
        %% the slot map. The delay is needed so that messages sent to the client
        %% are not lost in transit.
        {close_wait, non_neg_integer()}.


%%%===================================================================
%%% API
%%%===================================================================

%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-spec start_link([addr()], [opt()]) -> {ok, server_ref()} | {error, term()}.
%%
%% Start the cluster process. Clients will be set up to the provided
%% addresses and cluster information will be retrieved.
%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
start_link(Addrs, Opts) ->
    gen_server:start_link(?MODULE, [Addrs, Opts], []).

%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-spec stop(server_ref()) -> ok.
%%
%% Stop the cluster handling process and in turn disconnect and stop
%% all clients.
%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
stop(ServerRef) ->
    gen_server:stop(ServerRef).

%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-spec update_slots(server_ref(), non_neg_integer(), client_ref() | none) -> ok.
%%
%% Trigger a CLUSTER SLOTS command towards the specified Redis node if
%% the slot map version provided is the same as the one stored in the
%% cluster process state. This is used when a cluster state change is
%% detected with a MOVED redirection. It is also used when triggering
%% a slot update manually. In this case the node is 'none', meaning
%% no specific node is preferred.
%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
update_slots(ServerRef, SlotMapVersion, Node) ->
    gen_server:cast(ServerRef, {trigger_map_update, SlotMapVersion, Node}).

%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-spec get_slot_map_info(server_ref()) ->
          {SlotMapVersion :: non_neg_integer(),
           SlotMap :: ered_lib:slot_map(),
           Clients :: #{addr() => pid()}}.
%%
%% Fetch the cluster information. This provides the current slot map
%% and a map with all the clients.
%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
get_slot_map_info(ServerRef) ->
    gen_server:call(ServerRef, get_slot_map_info).

%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-spec connect_node(server_ref(), addr()) -> client_ref().
%%
%% Connect a client to the address and return a client reference. If a
%% client already exists for the address return a reference. This is
%% useful when a MOVE redirection is given to a address that has not
%% been seen before.
%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
connect_node(ServerRef, Addr) ->
    gen_server:call(ServerRef, {connect_node, Addr}).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([Addrs, Opts]) ->
    State = lists:foldl(
              fun ({info_pid, Val}, S)         -> S#st{info_pid = Val};
                  ({update_slot_wait, Val}, S) -> S#st{update_slot_wait = Val};
                  ({client_opts, Val}, S)      -> S#st{client_opts = Val};
                  ({min_replicas, Val}, S)     -> S#st{min_replicas = Val};
                  ({close_wait, Val}, S)       -> S#st{close_wait = Val};
                  (Other, _)                   -> error({badarg, Other})
              end,
              #st{initial_nodes = Addrs},
              Opts),
    {ok, start_clients(Addrs, State)}.


handle_call(get_slot_map_info, _From, State) ->
    Nodes = ered_lib:slotmap_all_nodes(State#st.slot_map),
    Clients = maps:with(Nodes, State#st.nodes),
    Reply = {State#st.slot_map_version, State#st.slot_map, Clients},
    {reply,Reply,State};

handle_call({connect_node, Addr}, _From, State) ->
    State1 = start_clients([Addr], State),
    ClientPid = maps:get(Addr, State1#st.nodes),
    {reply, ClientPid, State1}.

handle_cast({trigger_map_update, SlotMapVersion, Node}, State) ->
    case (SlotMapVersion == State#st.slot_map_version) and (State#st.slot_timer_ref == none) of
        true ->
            %% Get the address of the client. The address is needd to look up the node status
            %% before sending an update. This could need to go through all the nodes
            %% but it should not be done often enough to be a problem
            NodeAddr = case lists:keyfind(Node, 2, maps:to_list(State#st.nodes)) of
                           false ->
                               [];
                           {Addr, _Client} ->
                               [Addr]
                       end,
            {noreply, start_periodic_slot_info_request(NodeAddr, State)};
        false  ->
            {noreply, State}
    end.

handle_info(Msg = {connection_status, {_Pid, Addr, _Id} , Status}, State) ->
    IsMaster = sets:is_element(Addr, State#st.masters),
    ered_info_msg:connection_status(Msg, IsMaster, State#st.info_pid),
    State1 = case Status of
                 {connection_down, {socket_closed, _}} ->
                     %% Avoid triggering the alarm for a socket closed by the
                     %% peer. The cluster will be marked down on failed
                     %% reconnect or node down event.
                     State#st{reconnecting = sets:add_element(Addr, State#st.reconnecting)};
                 {connection_down,_} ->
                     State#st{up = sets:del_element(Addr, State#st.up),
                              pending = sets:del_element(Addr, State#st.pending),
                              reconnecting = sets:del_element(Addr, State#st.reconnecting)};
                 connection_up ->
                     State#st{up = sets:add_element(Addr, State#st.up),
                              pending = sets:del_element(Addr, State#st.pending),
                              reconnecting = sets:del_element(Addr, State#st.reconnecting)};
                 queue_full ->
                     State#st{queue_full = sets:add_element(Addr, State#st.queue_full)};
                 queue_ok ->
                     State#st{queue_full = sets:del_element(Addr, State#st.queue_full)}
             end,
    case check_cluster_status(State1) of
        ok ->
            %% Do not set the cluster state to OK yet. Wait for a slot info message.
            %% The slot info message will set the cluster state to OK if the map and
            %% connections are OK. This is to avoid to send the cluster OK info message
            %% too early if there are slot map updates in addition to connection errors.
            {noreply, State1};
        ClusterStatus ->
            {noreply, update_cluster_state(ClusterStatus, State1)}
    end;

handle_info({slot_info, Version, Response}, State) ->
    case Response of
        _ when Version < State#st.slot_map_version ->
            %% got a response for a request triggered for an old version of the slot map, ignore
            {noreply, State};
        {error, _} ->
            %% client error, i.e queue full or socket error or similar, ignore. New request will be sent periodically
            {noreply, State};
        {ok, {error, Error}} ->
            %% error sent from redis
            ered_info_msg:cluster_slots_error_response(Error, State#st.info_pid),
            {noreply, State};
        {ok, ClusterSlotsReply} ->
            NewMap = lists:sort(ClusterSlotsReply),
            case NewMap == State#st.slot_map of
                true ->
                    {noreply, update_cluster_state(State)};
                false ->
                    Nodes = ered_lib:slotmap_all_nodes(NewMap),
                    MasterNodes = new_set(ered_lib:slotmap_master_nodes(NewMap)),

                    %% remove nodes if they are not in the new map or initial.
                    Remove = maps:keys(lists:foldl(fun maps:without/2,
                                                   State#st.nodes,
                                                   [State#st.initial_nodes, Nodes])),

                    %% Deactivate the clients, so they can fail queued and new
                    %% commands immediately.
                    [ered_client:deactivate(maps:get(Addr, State#st.nodes)) || Addr <- Remove],

                    %% Stopping the clients is delayed to give time to update
                    %% slot map and to handle any messages in transit. If the
                    %% node comes back to the cluster soon enough, we can
                    %% reactivate these clients if they're not yet stopped.
                    TimerRef = erlang:start_timer(State#st.close_wait, self(), {close_clients, Remove}),
                    NewClosing = maps:merge(maps:from_list([{Addr, TimerRef} || Addr <- Remove]),
                                            State#st.closing),

                    ered_info_msg:slot_map_updated(ClusterSlotsReply, Version + 1, State#st.info_pid),

                    %% open new clients
                    State1 = start_clients(Nodes, State),
                    State2 = State1#st{slot_map_version = Version + 1,
                                       slot_map = NewMap,
                                       masters = MasterNodes,
                                       closing = NewClosing},
                    {noreply, update_cluster_state(State2)}
            end
    end;

handle_info({timeout, TimerRef, {time_to_update_slots,PreferredNodes}}, State) ->
    case State#st.slot_timer_ref of
        TimerRef when State#st.cluster_state == nok ->
            {noreply, start_periodic_slot_info_request(PreferredNodes,
                                                       State#st{slot_timer_ref = none})};
        TimerRef ->
            {noreply, State#st{slot_timer_ref = none}};
        _ ->
            {noreply, State}
    end;

handle_info({timeout, TimerRef, {close_clients, Remove}}, State) ->
    %% make sure they are still closing and mapped to this Timer
    ToCloseNow = [Addr ||
                     {Addr, Tref} <- maps:to_list(maps:with(Remove, State#st.closing)),
                     Tref == TimerRef],
    Clients = maps:with(ToCloseNow, State#st.nodes),
    [ered_client:stop(Client) || Client <- maps:values(Clients)],
    %% remove from nodes and closing map
    {noreply, State#st{nodes = maps:without(ToCloseNow, State#st.nodes),
                       up = sets:subtract(State#st.up, new_set(ToCloseNow)),
                       closing = maps:without(ToCloseNow, State#st.closing)}}.

terminate(_Reason, State) ->
    [ered_client:stop(Pid) || Pid <- maps:values(State#st.nodes)],
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

format_status(_Opt, Status) ->
    Status.

%%%===================================================================
%%% Internal functions
%%%===================================================================

-if(?OTP_RELEASE >= 24).
new_set(List) ->
    sets:from_list(List, [{version, 2}]).
-else.
new_set(List) ->
    sets:from_list(List).
-endif.

check_cluster_status(State) ->
    case is_slot_map_ok(State) of
        ok ->
            NonPendingMasters = sets:subtract(State#st.masters, State#st.pending),
            case sets:is_subset(NonPendingMasters, State#st.up) of
                false ->
                    master_down;
                true ->
                    case sets:is_disjoint(State#st.masters, State#st.queue_full) of
                        false ->
                            master_queue_full;
                        true ->
                            case sets:is_disjoint(State#st.masters, State#st.pending) of
                                false ->
                                    pending;
                                true ->
                                    ok
                            end
                    end
            end;
        Reason ->
            Reason
    end.

update_cluster_state(State) ->
    update_cluster_state(check_cluster_status(State), State).

update_cluster_state(ClusterStatus, State) ->
    case {ClusterStatus, State#st.cluster_state} of
        {ok, nok} ->
            ered_info_msg:cluster_ok(State#st.info_pid),
            State1 = stop_periodic_slot_info_request(State),
            State1#st{cluster_state = ok};
        {ok, ok} ->
            stop_periodic_slot_info_request(State);
        {pending, _} ->
            State;
        {_, ok} ->
            ered_info_msg:cluster_nok(ClusterStatus, State#st.info_pid),
            State1 = start_periodic_slot_info_request(State),
            State1#st{cluster_state = nok};
        {_, nok} ->
            start_periodic_slot_info_request(State)
    end.

start_periodic_slot_info_request(State) ->
    %% If we need to update the slot map due to a failover, it is likely that a
    %% replica of a failing master is located on a different machine
    %% (anti-affinity) and thus less likely to have crashed along with its
    %% master than other nodes.
    PreferredNodes = replicas_of_unavailable_masters(State),
    start_periodic_slot_info_request(PreferredNodes, State).

start_periodic_slot_info_request(PreferredNodes, State) ->
    case State#st.slot_timer_ref of
        none ->
            case pick_node(PreferredNodes, State) of
                none ->
                    %% try again when a node comes up
                    State;
                Node ->
                    send_slot_info_request(Node, State),
                    Tref = erlang:start_timer(
                             State#st.update_slot_wait,
                             self(),
                             {time_to_update_slots,PreferredNodes}),
                    State#st{slot_timer_ref = Tref}
            end;
        _Else ->
            State
    end.

stop_periodic_slot_info_request(State) ->
    case State#st.slot_timer_ref of
        none ->
            State;
        Tref ->
            erlang:cancel_timer(Tref),
            State#st{slot_timer_ref = none}
    end.

send_slot_info_request(Node, State) ->
    Pid = self(),
    Cb = fun(Answer) -> Pid ! {slot_info, State#st.slot_map_version, Answer} end,
    ered_client:command_async(Node, [<<"CLUSTER">>, <<"SLOTS">>], Cb).

%% Pick a random available node, preferring the ones in PreferredNodes if any of
%% them is available.
%%
%% Random is useful, since we may send multiple async CLUSTER SLOTS before we
%% get a reply for the first one, so we don't want the same node over and over.
pick_node(PreferredNodes, State) ->
    case pick_available_node(shuffle(PreferredNodes), State) of
        none ->
            %% No preferred node available. Pick one from the 'up' set.
            Addr = pick_available_node(shuffle(sets:to_list(State#st.up)), State);
        Addr ->
            Addr
    end,
    case Addr of
        none ->
            none;
        _ ->
            maps:get(Addr, State#st.nodes)
    end.

shuffle(List) ->
    [Y || {_, Y} <- lists:sort([{rand:uniform(16384), X} || X <- List])].

%% Pick node that is up and not queue full.
pick_available_node([Addr|Addrs], State) ->
    case node_is_available(Addr, State) of
        true ->
            Addr;
        false ->
            pick_available_node(Addrs, State)
    end;
pick_available_node([], _State) ->
    none.

node_is_available(Addr, State) ->
    sets:is_element(Addr, State#st.up) andalso
        not sets:is_element(Addr, State#st.queue_full) andalso
        not sets:is_element(Addr, State#st.reconnecting).

-spec replicas_of_unavailable_masters(#st{}) -> [addr()].
replicas_of_unavailable_masters(State) ->
    DownMasters = sets:subtract(State#st.masters, State#st.up),
    case sets:is_empty(DownMasters) of
        true ->
            [];
        false ->
            ered_lib:slotmap_replicas_of(DownMasters, State#st.slot_map)
    end.

is_slot_map_ok(State) ->
    %% Need at least two nodes in the cluster. During some startup scenarios it
    %% is possible to have a intermittent situation with only one node.
    if
        length(State#st.slot_map) < 2 ->
            too_few_nodes;
        true ->
            case all_slots_covered(State) of
                false ->
                    not_all_slots_covered;
                true ->
                    case check_replica_count(State) of
                        false ->
                            too_few_replicas;
                        true ->
                            ok
                    end
            end
    end.

all_slots_covered(State) ->
    %% check so that the slot map covers all slots. the slot map is sorted so it
    %% should be a continuous range
    R = lists:foldl(fun([Start, Stop| _Rest], Expect) ->
                            case Start of
                                Expect ->
                                    Stop+1;
                                _Else ->
                                    false
                            end
                    end,
                    0,
                    State#st.slot_map),
    %% check so last slot is ok
    R == 16384.

check_replica_count(State) ->
    lists:all(fun([_Start, _Stop, _Master | Replicas]) ->
                      length(Replicas) >= State#st.min_replicas
              end,
              State#st.slot_map).

start_client(Addr, State) ->
    {Host, Port} = Addr,
    Opts = [{info_pid, self()}, {use_cluster_id, true}] ++ State#st.client_opts,
    {ok, Pid} = ered_client:start_link(Host, Port, Opts),
    Pid.

start_clients(Addrs, State) ->
    %% open clients to new nodes not seen before
    %% cancel closing for requested clients
    {NewNodes, NewClosing} =
        lists:foldl(fun (Addr, {Nodes, Closing}) ->
                            case maps:find(Addr, Nodes) of
                                error ->
                                    Pid = start_client(Addr, State),
                                    {Nodes#{Addr => Pid}, Closing};
                                {ok, Pid} ->
                                    ered_client:reactivate(Pid),
                                    {Nodes, maps:remove(Addr, Closing)}
                            end
                    end,
                    {State#st.nodes, State#st.closing},
                    Addrs),

    State#st{nodes = maps:merge(State#st.nodes, NewNodes),
             pending = sets:union(State#st.pending,
                                  sets:subtract(new_set(maps:keys(NewNodes)),
                                                State#st.up)),
             closing = NewClosing}.
