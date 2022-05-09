-module(redis_cluster2).

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

%% from gen_sever.erl
-type server_ref() ::
        pid()
      | (LocalName :: atom())
      | {Name :: atom(), Node :: atom()}
      | {'global', GlobalName :: term()}
      | {'via', RegMod :: module(), ViaName :: term()}.

-type addr() :: redis_client:addr(). %{inet:socket_address()|inet:hostname(), inet:port_number()}.



-type opt() :: {any(), any()}.




%-type node_state() :: {init|connection_up|connection_down, pid()}.

-record(st, {% default_node :: undefined | {addr(), node_state()}, % will be used as default for slot map updates unless down

             cluster_state = nok :: ok | nok,
             initial_nodes = [] :: [addr()],
             nodes = #{} :: #{addr() => pid()}, % TODO, rename to clients?
             up = new_set([]),
             masters = new_set([]),
             queue_full = new_set([]),
             slot_map = [],
             slot_map_version = 1,
             slot_timer_ref = none,
%             node_ids = #{},

             info_pid = [] :: [pid()],
             update_delay = 1000, % 1s delay between slot map update requests
             client_opts = [],
             update_slot_wait = 500,
             min_replicas = 1,
             close_wait = 10000

            }).


% TODO

% [ ] setting to allow partial slot maps?
% [ ] DNS change?
% [ ] add node if to info
% [ ] Resolve DNS addr
% [ ] Add master-replica distinction in connection status/ connection status for replica


% node_id = #{id => sock_addr}
% node_addr = #{sock_addr => pid}
% node_status = #{pid => init|up|down}


%% -record(nd, {id, sock_addr, client_pid, status}






%%%===================================================================
%%% API
%%%===================================================================
start_link(Addrs, Opts) -> gen_server:start_link(?MODULE, [Addrs,
Opts], []).

stop(ServerRef) ->
    gen_server:stop(ServerRef).

update_slots(ServerRef, SlotMapVersion, Node) ->
    gen_server:cast(ServerRef, {trigger_map_update, SlotMapVersion, Node}).

% -------------------------------------------------------------------
-spec get_slot_map_info(server_ref()) -> 
          {
           SlotMapVersion :: non_neg_integer(),
           SlotMap :: redis_lib:slot_map(),
           Clients :: #{addr() => pid()}
          }.

get_slot_map_info(ServerRef) ->
    gen_server:call(ServerRef, get_slot_map_info).

connect_node(ServerRef, Addr) ->
    gen_server:call(ServerRef, {connect_node, Addr}).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%inet:ip_address() | inet:hostname()
init([Addrs, Opts]) ->
%    process_flag(trap_exit, true),
    State = lists:foldl(
              fun%% ({connection_opts, Val}, S) -> S#state{connection_opts = Val};
                 %% ({max_waiting, Val}, S)     -> S#state{waiting = q_new(Val)};
                 %% ({max_pending, Val}, S)     -> S#state{pending = q_new(Val)};
                 %% ({reconnect_wait, Val}, S)  -> S#state{reconnect_wait = Val};
                  ({info_pid, Val}, S)          -> S#st{info_pid = Val};
                  ({update_slot_wait, Val}, S) -> S#st{update_slot_wait = Val};
                  ({client_opts, Val}, S)     -> S#st{client_opts = Val};
                  ({min_replicas, Val}, S)     -> S#st{min_replicas = Val};
                  ({close_wait, Val}, S)     -> S#st{close_wait = Val};
                  (Other, _)                  -> error({badarg, Other})
              end,
              #st{},
              Opts),

    {ok, State#st{initial_nodes = Addrs,
                  nodes = maps:from_list([{Addr, start_client(Addr, State)} || Addr <- Addrs])}}.


handle_call(get_slot_map_info, _From, State) ->
    Nodes = redis_lib:slotmap_all_nodes(State#st.slot_map),
    Clients = maps:with(Nodes, State#st.nodes),
    Reply = {State#st.slot_map_version, State#st.slot_map, Clients},
    {reply,Reply,State};

handle_call({connect_node, Addr}, _From, State) ->
    case maps:get(Addr, State#st.nodes, not_found) of
        not_found ->
            ClientPid = start_client(Addr, State),
            {reply, ClientPid, State#st{nodes = maps:put(Addr, ClientPid, State#st.nodes)}};
        ClientPid ->
            {reply, ClientPid, State}
    end.

handle_cast({trigger_map_update, SlotMapVersion, Node}, State) ->
    case SlotMapVersion == State#st.slot_map_version of
        true ->
            {noreply, start_periodic_slot_info_request(Node, State)};
        false  ->
            {noreply, State}
    end.


handle_info(Msg = {connection_status, {Pid, Addr, _Id} , Status}, State) ->
    case maps:find(Addr, State#st.nodes) of
        {ok, Pid} ->
            IsMaster = sets:is_element(Addr, State#st.masters),
            redis_info_msg:connection_status(Msg, IsMaster, State#st.info_pid),
            State1 = case Status of
                         {connection_down,_} ->
                             State#st{up = sets:del_element(Addr, State#st.up)};
                         connection_up ->
                             State#st{up = sets:add_element(Addr, State#st.up)};
                         queue_full ->
                             State#st{queue_full = sets:add_element(Addr, State#st.queue_full)};
                         queue_ok ->
                             State#st{queue_full = sets:del_element(Addr, State#st.queue_full)};
                         {socket_closed, _} ->
                             State

                     end,
            {noreply, update_cluster_status(State1)};
        % old client
        _Other ->
            %% only interested in client_stopped messages. this client is defunct and if it
            %% comes back and gives a client up message it will just be confusing since it
            %% will be closed anyway
            [redis_info_msg:connection_status(Msg, _IsMaster = false, State#st.info_pid)
             || {connection_down, {client_stopped, _}} <- [Status]],
            {noreply, State}
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
            redis_info_msg:cluster_slots_error_response(Error, State#st.info_pid),
            {noreply, State};
        {ok, ClusterSlotsReply} ->
            NewMap = lists:sort(ClusterSlotsReply),
            case NewMap == State#st.slot_map of
                true ->
                    {noreply, State};
                false ->
                    Nodes = redis_lib:slotmap_all_nodes(NewMap),
                    MasterNodes = new_set(redis_lib:slotmap_master_nodes(NewMap)),

                    %% remove nodes if they are not in the new map or initial. Only remove nodes that
                    %% are already down to avoid closing a lot of clients if we get a transient slot map
                    %% missing nodes (might happen during Redis node startup I guess)
                    Remove = lists:foldl(fun maps:without/2,
                                         State#st.nodes,
                                         [State#st.initial_nodes, Nodes, sets:to_list(State#st.up)]),

                    %% these nodes already has clients
                    KeepNodes = maps:without(maps:keys(Remove), State#st.nodes),
                    %% open clients to new nodes not seen before
                    NewOpenNodes = maps:from_list([{Addr, start_client(Addr, State)}
                                                   || Addr <- Nodes,
                                                      not maps:is_key(Addr, State#st.nodes)]),

                    NewNodes = maps:merge(KeepNodes, NewOpenNodes),

                    redis_info_msg:slot_map_updated(ClusterSlotsReply, Version + 1, State#st.info_pid),

                    %% The close is delayed to give time to update slot map and to handle any
                    %% messages in transit
                    erlang:send_after(State#st.close_wait, self(), {close_clients, Remove}),

                    State1 = State#st{slot_map_version = Version + 1,
                                      slot_map = NewMap,
                                      masters = MasterNodes,
                                      nodes = maps:merge(KeepNodes, NewNodes)},
                    {noreply, update_cluster_status(State1)}
            end
    end;

handle_info({timeout, TimerRef, time_to_update_slots}, State) ->
    case State#st.slot_timer_ref of
        TimerRef when State#st.cluster_state == nok ->
            {noreply, start_periodic_slot_info_request(State#st{slot_timer_ref = none})};
        TimerRef ->
            {noreply, State#st{slot_timer_ref = none}};
        _ ->
            {noreply, State}
    end;

handle_info({close_clients, Remove}, State) ->
    [redis_client:stop(ClientPid) || ClientPid <- maps:values(Remove)],
    {noreply, State}.

terminate(_Reason, State) ->
    [redis_client:stop(Pid) || Pid <- maps:values(State#st.nodes)],
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

format_status(_Opt, Status) ->
    Status.

%%%===================================================================
%%% Internal functions
%%%===================================================================

new_set(List) ->
    sets:from_list(List).
    % sets:from_list(List, [{version, 2}]). TODO: OTP 24

update_cluster_status(State) ->
    case is_slot_map_ok(State) of
        ok ->
            case sets:is_subset(State#st.masters, State#st.up) of
                false ->
                    set_cluster_state(nok, master_down, State);
                true ->
                    case sets:is_disjoint(State#st.masters, State#st.queue_full) of
                        false ->
                            set_cluster_state(nok, master_queue_full, State);
                        true ->
                            set_cluster_state(ok, ok, State)
                    end
            end;
        Reason ->
            set_cluster_state(nok, Reason, State)
    end.

set_cluster_state(nok, Reason, State) ->
    State1 = case State#st.cluster_state of
                 nok ->
                     State;
                 ok ->
                     redis_info_msg:cluster_nok(Reason, State#st.info_pid),
                     State#st{cluster_state = nok}
             end,
    start_periodic_slot_info_request(State1);

set_cluster_state(ok, _, State) ->
    State1 = case State#st.cluster_state of
                 nok ->
                     redis_info_msg:cluster_ok(State#st.info_pid),
                     State#st{cluster_state = ok};
                 ok ->
                     State
             end,
    stop_periodic_slot_info_request(State1).


start_periodic_slot_info_request(State) ->
    case pick_node(State) of
        none ->
            % try again when a node comes up
            State;
        Node ->
            start_periodic_slot_info_request(Node, State)
    end.

start_periodic_slot_info_request(Node, State) ->
    case State#st.slot_timer_ref of
        none ->
            send_slot_info_request(Node, State),
            Tref = erlang:start_timer(State#st.update_slot_wait, self(), time_to_update_slots),
            State#st{slot_timer_ref = Tref};
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
    redis_client:command_async(Node, [<<"CLUSTER">>, <<"SLOTS">>], Cb).



pick_node(State) ->
    case sets:is_empty(State#st.up) of
        true ->
            none;
        false ->
            %% prioritize initial configured nodes
            case lists:dropwhile(fun(Addr) -> not sets:is_element(Addr, State#st.up) end,
                                 State#st.initial_nodes) of
                [] ->
                    %% no initial node up, pick one from the up set
                    Addr = hd(sets:to_list(State#st.up));
                [Addr|_] ->
                    Addr
            end,
            maps:get(Addr, State#st.nodes)
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
    {ok, Pid} = redis_client:start_link(Host, Port, Opts),
    Pid.



