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
%-type node_state() :: {init|connection_up|connection_down, pid()}.

-record(st, {% default_node :: undefined | {addr(), node_state()}, % will be used as default for slot map updates unless down

             cluster_state = nok :: ok | nok,
             initial_nodes = [] :: [addr()],
             nodes = #{} :: #{addr() => pid()}, 
             up = new_set([]),
             masters = new_set([]),
             slot_map = [],
             slot_map_version = 1,
             slot_timer_ref = none,
%             node_ids = #{},

             info_cb = none,
             update_delay = 1000, % 1s delay between slot map update requests
             client_opts = [],
             update_slot_wait = 500

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
start_link(Host, Port, Opts) ->
    gen_server:start_link(?MODULE, [Host, Port, Opts], []).

stop(ServerRef) ->
    gen_server:stop(ServerRef).

update_slots(ServerRef, SlotMapVersion, Node) ->
    gen_server:cast(ServerRef, {trigger_map_update, SlotMapVersion, Node}).

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
                  ({info_cb, Val}, S)        -> S#st{info_cb = Val};
                  ({update_slot_wait, Val}, S)        -> S#st{update_slot_wait = Val};
                  ({client_opts, Val}, S)     -> S#st{client_opts = Val};
                  (Other, _)                  -> error({badarg, Other})
              end,
              #st{},
              Opts),

    {ok, State#st{initial_nodes = Addrs,
                  nodes = maps:from_list([{Addr, start_client(Addr, State)} || Addr <- Addrs])}}.


handle_call(Request, _From, State) ->
    {stop, {unexpected_call, Request}, State}.


handle_cast({trigger_map_update, SlotMapVersion, Node}, State) ->
    case SlotMapVersion == State#st.slot_map_version of
        true ->
            {noreply, start_periodic_slot_info_request(Node, State)};
        false  ->
            {noreply, State}
    end.


handle_info(Msg = {connection_status, {_Pid, Addr, _Id} , Status}, State) ->
    send_info(Msg, State),
    State1 = State#st{up = case Status of
                               connection_down ->
                                   sets:del_element(Addr, State#st.up);
                               connection_up ->
                                   sets:add_element(Addr, State#st.up)
                           end},
    {noreply, update_cluster_status(State1)};


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
                    Nodes = redis_lib:slotmap_all_nodes(NewMap),
                    MasterNodes = new_set(redis_lib:slotmap_master_nodes(NewMap)),

                    %% remove nodes if they are not in the new map or if they are initial.
                    Remove = maps:without(Nodes, maps:without(State#st.initial_nodes, State#st.nodes)),
                    %% these nodes already has clients
                    KeepNodes = maps:without(maps:keys(Remove), State#st.nodes),
                    %% open clients to new nodes not seen before
                    NewOpenNodes = maps:from_list([{Addr, start_client(Addr, State)}
                                                   || Addr <- Nodes,
                                                      maps:is_key(Addr, State#st.nodes)]),

                    NewNodes = maps:merge(KeepNodes, NewOpenNodes),
                    send_info({slot_map_updated, {ClusterSlotsReply, NewNodes, Version + 1}}, State),

                    %% Important to wait with closing nodes until the slot infor is sent out. We
                    %% want to make sure that slot maps are updated before closing otherwise
                    %% messages might be routed to missing processes.
                    [redis_client:stop(ClientPid) || ClientPid <- maps:values(Remove)],

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
    end.


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
    sets:from_list(List, [{version, 2}]).

update_cluster_status(State) ->
    case is_slot_map_ok(State) of
        false ->
            set_cluster_state(nok, bad_slot_map, State);
        true ->
            case sets:is_subset(State#st.masters, State#st.up) of
                false ->
                    set_cluster_state(nok, master_down, State);
                true ->
                    set_cluster_state(ok, ok, State)
            end
    end.

set_cluster_state(nok, _Reason, State) ->
    State1 = case State#st.cluster_state of
                 nok ->
                     State;
                 ok ->
                     State#st{cluster_state = nok}  % send_info(#{msg_type := cluster_not_ok,
             end,
    start_periodic_slot_info_request(State1);

set_cluster_state(ok, _, State) ->
    State1 = case State#st.cluster_state of
                 nok ->
                     send_info({connection_status, self(), fully_connected}, State),
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
    redis_client:request_cb(Node, [<<"CLUSTER">>, <<"SLOTS">>], Cb ).



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

% socket error {node_type => master|replica}

% connect failed
% connect ok
% connect wait timeout


% slot map updated
% slot map error

% all master nodes ok
% all master nodes not ok

% queue overflow
% queue level below watermark


-type info_msg() ::

        #{msg_type := connection_closed,
          reason := any(),
          node_type := master|replica,
          ip := inet:socket_address(),
          port := inet:port()} |

        #{msg_type := connect_error,
          reason := any(),
          node_type := master|replica,
          ip := inet:socket_address(),
          port := inet:port()} |

        #{msg_type := connect_ok,
          node_type := master|replica,
          ip := inet:socket_address(),
          port := inet:port()} |

        #{msg_type := node_flagged_as_down,
          node_type := master|replica,
          ip := inet:socket_address(),
          port := inet:port()}.



-type node_info(Reason, ErrorCode) ::
        #{msg_type := node_info,
          reason := Reason,
          error_code:= ErrorCode,
          node_type := master|replica,
          ip := inet:socket_address(),
          port := inet:port()}.

-type info_msg2() ::
        node_info(connect_succsesful, none) |

        node_info(socket_closed, any()) |

        node_info(connect_error, any()) |

        node_info(flagged_as_down, gone_too_long) |

        node_info(queue_below_watermark, none) |

        node_info(queue_overflow, none) |

        #{msg_type := slot_map_updated,
          slot_map := ClusterSlotsReply :: any(),
          map_version := non_neg_integer()} |

        #{msg_type := slot_map_error_response,
          response := RedisReply :: any()} |

        #{msg_type := cluster_ok} |

        #{msg_type := cluster_not_ok,
          reason := master_node_down | master_node_queue_full}.


% all master nodes not ok




send_info(Msg, State = #st{info_cb = Fun}) ->
    [Fun(Msg) || Fun /= none],
    State.


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
    {Host, Port} = Addr,
    Opts = [{info_pid, self()}] ++ State#st.client_opts,
    {ok, Pid} = redis_client:start_link(Host, Port, Opts),
    Pid.
