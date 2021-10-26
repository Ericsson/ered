-module(redis_cluster2).

-behaviour(gen_server).

%% API
-export([start_link/3,
         stop/1,
        update_slots/3]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3, format_status/2]).


-type addr() :: {inet:socket_address()|inet:hostname(), inet:port_number()}.
-type node_state() :: {init|connection_up|connection_down, pid()}.

-record(st, {default_node :: undefined | {addr(), node_state()}, % will be used as default for slot map updates unless down
             update_delay = 1000, % 1s delay between slot map update requests
             client_opts = [],
             nodes = #{} :: #{addr() => node_state()},  %  socket address {IP, Port} => {Pid, init | up | down}
%             node_ids = #{},
             info_cb = none,
             slot_map = [],
             slot_map_version = 1,
             timer_ref = none,
             update_wait = 500
            }).


% TODO

% [ ] setting to allow partial slot maps?
% [ ] DNS change?
% [ ] add node if to info
% [ ] Resolve DNS addr


% node_id = #{id => sock_addr}
% node_addr = #{sock_addr => pid}
% node_status = #{pid => init|up|down}


%% -record(nd, {id, sock_addr, client_pid, status}

%%%===================================================================
%%% API
%%%===================================================================
start_link(Host, Port, Opts) ->
    gen_server:start_link(?MODULE, [Host, Port, Opts], []).

stop(ServerRef) ->
    gen_server:stop(ServerRef).

update_slots(ServerRef, SlotMapVersion, Node) ->
    gen_server:cast(ServerRef, {trigger_map_update, SlotMapVersion, Node}).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================
init([Host, Port, Opts]) ->
%    process_flag(trap_exit, true),
    State = lists:foldl(
              fun%% ({connection_opts, Val}, S) -> S#state{connection_opts = Val};
                 %% ({max_waiting, Val}, S)     -> S#state{waiting = q_new(Val)};
                 %% ({max_pending, Val}, S)     -> S#state{pending = q_new(Val)};
                 %% ({reconnect_wait, Val}, S)  -> S#state{reconnect_wait = Val};
                  ({info_cb, Val}, S)        -> S#st{info_cb = Val};
                  ({client_opts, Val}, S)     -> S#st{client_opts = Val};
                  (Other, _)                  -> error({badarg, Other})
              end,
              #st{},
              Opts),
    {ok, Pid} = redis_client:start_link(Host, Port, [{info_pid, self()}] ++ State#st.client_opts),
    Addr = {Host, Port},
    NodeState = {init, Pid},
    {ok, State#st{default_node = {Addr, NodeState}}}.


handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

handle_cast({trigger_map_update, SlotMapVersion, Node}, State) ->
    case SlotMapVersion == State#st.slot_map_version of
        true ->
            %% see so the node is up
            case lists:member(Node, pick_node(State)) of
                true ->
                    {noreply, start_periodic_slot_info_request(Node, State)};
                false ->
                    {noreply, State}
            end;
        false  ->
            {noreply, State}
    end.

%% TODO create handle_connection_status function?
handle_info(Msg = {connection_status, Source, Status}, State) ->
    case is_known_node(Source, State) of
        true ->
            send_info(Msg, State),
            State1 = set_node_status(Source, Status, State),
            {noreply, check_if_all_is_ok(State1)};
        false ->
            {noreply, State}
    end;

handle_info({slot_info, Version, Response}, State) ->
    case Response of
        _ when Version < State#st.slot_map_version ->
            %% got a response for a request triggered for an old version of the slot map, ignore
            {noreply, State};
        {error, _} ->
            %% client error, i.e queue full or similar, ignore. New request will be sent periodically
            {noreply, State};
        {ok, {error, Reason}} ->
            %% error sent from redis
            %exit(slot_info_error_from_redis); % TODO: handle
            send_info({slot_info_error_from_redis, Reason}, State),
            {noreply, State};
        {ok, ClusterSlotsReply} ->
            NewMap = lists:sort(ClusterSlotsReply),
            case NewMap == State#st.slot_map of
                true ->
                    {noreply, State};
                false ->
                    Nodes = [Addr || {_SlotStart, _SlotEnd, Addr} <- redis_lib:parse_cluster_slots(NewMap)],
                    State1 = connect_nodes(Nodes, State),
                    Old = maps:keys(State1#st.nodes),
                    %% Nodes to be closed. Do not include when sending slot info but do wait to
                    %% close the connection until the slot info is sent out. We want to make
                    %% sure that slot maps are updated before closing otherwise messages might
                    %% be routed to missing processes.
                    Remove = Old -- Nodes,
                    send_slot_info(Remove, ClusterSlotsReply, State1),
                    State2 = disconnect_old_nodes(Remove, State1),
                    State3 = State2#st{slot_map_version = Version + 1, slot_map = NewMap},
                    {noreply, check_if_all_is_ok(State3)}
            end
    end;

handle_info({timeout, TimerRef, time_to_update_slots}, State) ->
    case State#st.timer_ref of
        TimerRef ->
            State1 = State#st{timer_ref = none},
            case is_slot_map_ok(State1) andalso all_master_nodes_up(State1) of
                false ->
                    {noreply, start_periodic_slot_info_request(State1)};
                true ->
                    {noreply, State1}
            end;
        _ ->
            {noreply, State}
    end.

%% handle_info({'EXIT', _Pid , normal}, State) ->
%%     {noreply, State}.

terminate(_Reason, State) ->
    Nodes = [State#st.default_node | maps:to_list(State#st.nodes)],
    [redis_client:stop(Pid) || {_Host, {_ClientState, Pid}} <- Nodes],
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

format_status(_Opt, Status) ->
    Status.

%%%===================================================================
%%% Internal functions
%%%===================================================================
check_if_all_is_ok(State) ->
    case is_slot_map_ok(State) andalso all_master_nodes_up(State) of
        true ->
            send_info({connection_status, self(), fully_connected}, State),
            stop_periodic_slot_info_request(State);
        false ->
            start_periodic_slot_info_request(State)
    end.

connect_nodes(Nodes, State) ->
    lists:foldl(fun start_client/2, State, Nodes).

send_slot_info(Remove, ClusterSlotsReply, State) ->
    NodeProcs = maps:map(fun(_K, {_Status, Pid}) -> Pid end, maps:without(Remove, State#st.nodes)),
    MapVersion = State#st.slot_map_version + 1,
    send_info({slot_map_updated, {ClusterSlotsReply, NodeProcs, MapVersion}}, State).

disconnect_old_nodes(Remove, State) ->
    lists:foldl(fun stop_client/2, State, Remove).


start_periodic_slot_info_request(State) ->
    case pick_node(State) of
        [] ->
            % try again when a node comes up
            State;
        [Node|_] ->
            start_periodic_slot_info_request(Node, State)
    end.

start_periodic_slot_info_request(Node, State) ->
    case State#st.timer_ref of
        none ->
            send_slot_info_request(Node, State),
            Tref = erlang:start_timer(State#st.update_wait, self(), time_to_update_slots),
            State#st{timer_ref = Tref};
        _Else ->
            State
    end.

stop_periodic_slot_info_request(State) ->
    case State#st.timer_ref of
        none ->
            State;
        Tref ->
            timer:cancel(Tref),
            State#st{timer_ref = none}
    end.


send_slot_info_request(Node, State) ->
    Pid = self(),
    Cb = fun(Answer) ->  Pid ! {slot_info, State#st.slot_map_version, Answer} end,
    redis_client:request_cb(Node, [<<"CLUSTER">>, <<"SLOTS">>], Cb ).



pick_node(State) ->
    %% prioritize default node
    Nodes = [State#st.default_node | lists:sort(maps:to_list(State#st.nodes))],
    [Pid || {_Host, {connection_up, Pid}} <- Nodes].


send_info(Msg, #st{info_cb = Fun}) ->
    [Fun(Msg) || Fun /= none],
    ok.


is_known_node({Pid, Addr, _Id}, State) ->
    case State#st.default_node of
        {Addr, {_, Pid}} ->
            true;
        _ ->
            maps:is_key(Addr, State#st.nodes)
    end.

set_node_status({Pid, Addr, _Id}, Status, State) ->
    case State#st.default_node of
        {Addr, {_, Pid}} ->
            State#st{default_node = {Addr, {Status, Pid}}};
        _ ->
            State#st{nodes = maps:put(Addr, {Status, Pid}, State#st.nodes)}
    end.

%% TODO change to node down?
all_master_nodes_up(State) ->
    MasterNodes = maps:with(redis_lib:slotmap_master_nodes(State#st.slot_map), State#st.nodes),
    lists:all(fun({Status, _Pid}) -> Status == connection_up end, maps:values(MasterNodes)).

%% all_nodes_up(State) ->
%%     lists:all(fun({Status, _Pid}) -> Status == connection_up end, maps:values(State#st.nodes)).

is_slot_map_ok(State) ->
    %% Need at least two nodes in the cluster. During some startup scenarios it
    %% is possible to have a intermittent situation with only one node.
    length(State#st.slot_map) >= 2 andalso all_slots_covered(State).

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


start_client(Addr, State) ->
    case maps:is_key(Addr, State#st.nodes) of
        true ->
            State;
        false ->
            {Host, Port} = Addr,
            Opts = [{info_pid, self()}] ++ State#st.client_opts,
            {ok, Pid} = redis_client:start_link(Host, Port, Opts),
            State#st{nodes = maps:put(Addr, {init, Pid}, State#st.nodes)}
    end.

stop_client(Addr, State) ->
    %% It should not be possible to get an error here I think..
    {{_NodeState, Pid}, NewNodes} = maps:take(Addr, State#st.nodes),
    redis_client:stop(Pid),
    State#st{nodes = NewNodes}.


