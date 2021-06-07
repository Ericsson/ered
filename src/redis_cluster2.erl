-module(redis_cluster2).

-behaviour(gen_server).

%% API
-export([start_link/3]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3, format_status/2]).


-type addr() :: {inet:socket_address(), inet:port_number()}.
-type node_state() :: {init | connection_up | connection_down, pid()}.

-record(st, {default_node :: {addr(), node_state()}, % will be used as default for slot map updates unless down
             update_delay = 1000, % 1s delay between slot map update requests
             client_opts = [],
             nodes = #{} :: #{addr() => node_state()},  %  socket address {IP, Port} => {Pid, init | up | down}
%             node_ids = #{},
             info_cb = none,
             slot_map = [],
             slot_map_version = 0,
             timer_ref = none,
             update_wait = 500
            }).


% TODO

% [ ] setting to allow partial slot maps?
% [ ] DNS change?
% [ ] add node if to info


% node_id = #{id => sock_addr}
% node_addr = #{sock_addr => pid}
% node_status = #{pid => init|up|down}


%% -record(nd, {id, sock_addr, client_pid, status}

%%%===================================================================
%%% API
%%%===================================================================
start_link(Host, Port, Opts) ->
    gen_server:start_link(?MODULE, [Host, Port, Opts], []).

%% trigger_map_update(Node) ->
%%     gen_server:cast(?MODULE, {trigger_map_update, Node}).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================
init([Host, Port, Opts]) ->

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


%% handle_info({Pid, Addr, Id}, connection_up, State) ->
%%     State1 = State#st{nodes = maps:put(Addr, {connection_up, Pid, Id}, State#st.nodes}},
%%     State2 = update_map(State1),
%%     State3 = report_status(State2),
%%     {noreply, State3};
handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

handle_cast(_Request, State) ->
    {noreply, State}.


%% TODO create handle_connection_status function?
handle_info(Msg = {connection_status, Source, Status}, State) ->
    %% TODO check if node is in map
    send_info(Msg, State),
    State1 = set_node_status(Source, Status, State),
    case is_slot_map_ok(State1) andalso all_nodes_up(State1) of
        true ->
            send_info({connection_status, self(), fully_connected}, State1),
            {noreply, stop_periodic_slot_info_request(State1)};
        false ->
            {noreply, start_periodic_slot_info_request(State1)}
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
            NewMap = parse_cluster_slots(ClusterSlotsReply),
            case NewMap == State#st.slot_map of
                true ->
                    {noreply, State};
                false ->
                    Nodes = [Addr || {_SlotStart, _SlotEnd, Addr} <- NewMap],
                    State1 = connect_nodes(Nodes, State),
                    Old = maps:keys(State1#st.nodes),
                    %% Nodes to be closed. Do not include when sending slot info but do wait to
                    %% close the connection until the slot info is sent out. We want to make
                    %% sure that slot maps are updated before closing otherwise messages might
                    %% be routed to missing processes.
                    Remove = Old -- Nodes,
                    send_slot_info(Remove, ClusterSlotsReply, State1),
                    State2 = disconnect_old_nodes(Remove, State1),
                    {noreply, State2#st{slot_map_version = Version + 1,
                                        slot_map = NewMap}}
            end
    end;

handle_info({timeout, TimerRef, time_to_update_slots}, State) ->
    case State#st.timer_ref of
        TimerRef ->
            {noreply, start_periodic_slot_info_request(State#st{timer_ref = none})};
        _ ->
            {noreply, State}
    end.



connect_nodes(Nodes, State) ->
    lists:foldl(fun start_client/2, State, Nodes).

send_slot_info(Remove, ClusterSlotsReply, State) ->
    NodeProcs = maps:map(fun(_K, {_Status, Pid}) -> Pid end, maps:without(Remove, State#st.nodes)),
    send_info({slot_map_updated, {ClusterSlotsReply, NodeProcs}}, State).

disconnect_old_nodes(Remove, State) ->
    lists:foldl(fun stop_client/2, State, Remove).


parse_cluster_slots(ClusterSlotsReply) ->

    %% [[10923,16383,
    %%   [<<"127.0.0.1">>,30003,
    %%    <<"3d87c864459cb190be1a272e6096435e87721c94">>],
    %%   [<<"127.0.0.1">>,30006,
    %%    <<"12d0ac6c30fcbec08555831bf81afe8d5c0c1d4b">>]],
    %%  [0,5460,
    %%   [<<"127.0.0.1">>,30001,
    %%    <<"1127e053184e563727ee7d10f1f4851127f6f064">>],
    %%   [<<"127.0.0.1">>,30004,
    %%    <<"2dc6838de2543a104b623bd986013e24e7260eb6">>]],
    %%  [5461,10922,
    %%   [<<"127.0.0.1">>,30002,
    %%    <<"848879a1027f7a95ea058f3ca13a08bf4a70d7db">>],
    %%   [<<"127.0.0.1">>,30005,
    %%    <<"6ef9ab5fc9b66b63b469c5f53978a237e65d42ce">>]]]

    %% TODO: Maybe wrap this in a try catch if we get garbage?
    SlotMap = [{SlotStart, SlotEnd, {binary_to_list(Ip), Port}}
               || [SlotStart, SlotEnd, [Ip, Port |_] | _] <- ClusterSlotsReply],
    lists:sort(SlotMap).


start_periodic_slot_info_request(State = #st{timer_ref = none}) ->
    case pick_node(State) of
        [] ->
            % try again when a node comes up
            State;
        [Node|_] ->
            send_slot_info_request(Node, State),
            Tref = erlang:start_timer(State#st.update_wait, self(), time_to_update_slots),
            State#st{timer_ref = Tref}
    end;
start_periodic_slot_info_request(State) ->
    State.


stop_periodic_slot_info_request(State) ->
    case State#st.timer_ref of
        none ->
            State;
        Tref ->
            timer:cancel(Tref),
            State#st{timer_ref = none}
    end.

%% update_slot_map(State) ->
%%     Node = pick_node(State)
%%     update_slot_map(Node, State).

send_slot_info_request(Node, State) ->
    Pid = self(),
    Cb = fun(Answer) ->  Pid ! {slot_info, State#st.slot_map_version, Answer} end,
    redis_client:request_cb(Node, [<<"CLUSTER">>, <<"SLOTS">>], Cb ).


%% pick_node(State) ->
%%     case State#st.default_node of
%%         {connection_up, Pid} ->
%%             [Pid];
%%         _  ->
%%             [Pid || {connection_up, Pid} <- lists:sorted(maps:to_list(State#st.nodes))]
%%     end.


pick_node(State) ->
    %% prioritize default node
    Nodes = [State#st.default_node | lists:sort(maps:to_list(State#st.nodes))],
    [Pid || {_Host, {connection_up, Pid}} <- Nodes].

%% pick_node(State) ->
%%     %% TODO: How to deal with default_node host name not ip? Can there be a race where there
%%     %% are two connections to the same server? What if default is slave?
%%     case maps:get(State#st.default_node, State#st.nodes, undefined) of
%%         {connection_up, Pid} ->
%%             [Pid];
%%         _  ->
%%             [Pid || {connection_up, Pid} <- lists:sorted(maps:to_list(State#st.nodes))]
%%     end.

send_info(Msg, #st{info_cb = Fun}) ->
    [Fun(Msg) || Fun /= none],
    ok.

set_node_status({Pid, Addr, _Id}, Status, State) ->
    case State#st.default_node of
        {Addr, {_, Pid}} ->
            State#st{default_node = {Addr, {Status, Pid}}};
        _ ->
    %        exit({ State#st.default_node, Pid, Addr}),
            State#st{nodes = maps:put(Addr, {Status, Pid}, State#st.nodes)}
    end.

%% TODO change to node down?
all_nodes_up(State) ->
    lists:all(fun({Status, _Pid}) -> Status == connection_up end, maps:values(State#st.nodes)).
%    lists:all([Status == connection_up || {Status, Pid} <- maps:values(State#st.nodes)]).

    %% Pred = fun(_Add, {Status, _Pid}) -> Status /= connection_up end,
    %% maps:filter(Pred, State#st.nodes) == #{}.

%% is_slot_map_ok(State) ->
%%     lists:all(fun({_Slot, Node}) -> Node /= unmapped end, State#st.slot_map)
%%         andalso State#st.slot_map /= [].

%% is_continuous(16383, []) ->
%%     true;
%% is_continuous(Prev, [{Start, Stop} | Rest]) ->
%%     case Prev + 1 of
%%         Start ->
%%             is_continuous(Stop, Rest);
%%         _Else ->
%%             false
%%     end.

is_slot_map_ok(State) ->
    %% check so that the slot map covers all slots. the slot map is sorted so it
    %% should be a continuous range
    R = lists:foldl(fun({Start, Stop, _Addr}, Expect) ->
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

% MERGE?
%% handle_info({Source, connection_up}, State) ->
%%     {Pid, Addr, Id} = Source,
%%     report_status({connection_up, Addr, Id}, State),
%%     State1 = set_node(Addr, {connection_up, Pid, Id}),
%%     case {slot_map_state(State1), node_state(State1)} of
%%      {ok, ok} ->
%%          report_status(all_up, State1),
%%          {noreply, stop_periodic_update_slot_map(State1)};
%%      {_, _} ->
%%          {noreply, start_periodic_update_slot_map(State1)}
%%     end;

%% handle_info({Source, connection_down}, State) ->
%%     {Pid, Addr, Id} = Source,
%%     report_status({connection_down, Addr, Id}, State),
%%     State1 = set_node(Addr, {connection_down, Pid, Id}),
%%     {noreply, start_periodic_update_slot_map(State1)}.



%% handle_info({'EXIT', _Pid, Reason}, State) ->
%%     {noreply, connection_error(Reason, State)};

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

format_status(_Opt, Status) ->
    Status.
%%%===================================================================
%%% Internal functions
%%%===================================================================
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
    error(todo).

%% update_slot_map(Node, State) ->
%%     Now = erlang:monotonic_time(milli_seconds),
%%     TimeSinceLast = Now - maps:get(Node, State#st.update_time, 0),
%%     SleepTime = case State#st.update_delay - TimeSinceLast of
%%                  X when X <= 0 -> 0;
%%                  X -> X
%%              end,
%%     Pid = spawn_link(fun() ->
%%                           timer:sleep(SleepTime),
%%                           Connection = redis_connection:connect(Host, Port, [{connect_timeout, State#st.update_delay}]),
%%                           Result = redis_connection:request(Connection, <<"CLUSTER", "SLOTS">>),
%%                           exit({slot_map, Result})
%%                   end),
%%     St#st{proc_pid = Pid,
%%        update_time = maps:put(Node, Now + SleepTime, State#st.update_time)}.

