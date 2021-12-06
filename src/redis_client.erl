-module(redis_client).

-behaviour(gen_server).

%% API



-export([start_link/3,
         stop/1,
         request/2, request/3,
         request_cb/3]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3, format_status/2]).

-record(state, {host,
                port,
                connection_opts = [],
                info_pid = none,
                resp_version = 3,
                reconnect_wait = 1000,
                pending_timeout = 3000 :: non_neg_integer(),
                use_cluster_id = false :: boolean(),

                connection_pid = none,

                waiting = q_new(5000),
                pending = q_new(128),
                connection_state = initial, % initial, up, down, pending
                cluster_id = undefined :: undefined | binary(),

                pending_timer = none :: none | reference(), %% Timer to go from pending to down
                init_timer = none :: none | reference() %% Timer to retry the init procedure
               }
       ).

-type host()        :: inet:socket_address() | inet:hostname().
-type addr()        :: {host(), inet:port_number()}.
-type node_id()     :: binary() | undefined.
-type client_info() :: {pid(), addr(), node_id()}.
-type status()      :: connection_up | {connection_down, down_reason()}.
-type reason()      :: term(). % ssl reasons are of type any so no point being more specific
-type down_reason() :: {client_stopped | connect_error | init_error | socket_closed, reason()}.
%-type down_reason() :: {client_stopped, reason()} | {connect_error, reason()} | {init_error, reason()} | {socket_closed, reason()}.
-type info_msg()    :: {connection_status, client_info(), status()}.

-export_type([info_msg/0, addr/0]).

%%%===================================================================
%%% API
%%%===================================================================
start_link(Host, Port, Opts) ->
    gen_server:start_link(?MODULE, [Host, Port, Opts], []).

stop(ServerRef) ->
    gen_server:stop(ServerRef).

request(ServerRef, Request) ->
    request(ServerRef, Request, infinity).

request(ServerRef, Request, Timeout) ->
    gen_server:call(ServerRef, {request, redis_lib:format_request(Request)}, Timeout).

request_cb(ServerRef, Request, CallbackFun) ->
    gen_server:cast(ServerRef, {request, redis_lib:format_request(Request), CallbackFun}).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================
init([Host, Port, Opts]) ->
    %% process_flag(trap_exit, true),
    State = lists:foldl(
              fun({connection_opts, Val}, S) -> S#state{connection_opts = Val};
                 ({max_waiting, Val}, S)     -> S#state{waiting = q_new(Val)};
                 ({max_pending, Val}, S)     -> S#state{pending = q_new(Val)};
                 ({reconnect_wait, Val}, S)  -> S#state{reconnect_wait = Val};
                 ({info_pid, Val}, S)        -> S#state{info_pid = Val};
                 ({resp_version, Val}, S)    -> S#state{resp_version = Val};
                 ({pending_timeout, Val}, S) -> S#state{pending_timeout = Val};
                 ({use_cluster_id, Val}, S)  -> S#state{use_cluster_id = Val};
                 (Other, _)                  -> error({badarg, Other})
              end,
              #state{host = Host, port = Port},
              Opts),
    {ok, start_connect(State)}.

handle_call({request, Request}, From, State) ->
    Fun = fun(Reply) -> gen_server:reply(From, Reply) end,
    handle_cast({request, Request, Fun}, State).

handle_cast(Request, State) ->
    {noreply, new_request(Request, State)}.

handle_info({request_reply, Reply}, State = #state{pending = Pending}) ->
          {Request, NewPending} = q_out(Pending),
    reply_request(Request, {ok, Reply}),
    {noreply, send_waiting(State#state{pending = NewPending})};


%% handle_info({init_request_reply, Reply}, State) ->
%%     case Reply of
%%         {error, Reason} ->
%%             {noreply, init_error({init_error, Reason}, State)};
%%         _ ->
%%             %erlang:cancel_timer(State#state.pending_timer),
%%             report_connection_status(connection_up, State),
%%             {noreply, send_waiting(State#state{connection_state = up,
%%                                                pending_timer = none})}
%%     end;

handle_info({init_request_reply, Reply}, State) ->
   % io:format("~p\n", [Reply]),
    Result = case Reply of
                 [] ->
                     {ok,State};
                 [HelloReply | Rest] ->
                     case HelloReply of
                         {error, _} ->
                             error;
                         _ ->
                             case Rest of
                                 [] ->
                                     {ok,State};
                                 [{error,_}] ->
                                     error;
                                 [ClusterId] ->
                                     {ok, State#state{cluster_id = ClusterId}}
                             end
                     end
        end,
    case Result of
        error ->
            {noreply, init_error({init_error, Reply}, State)};
        {ok, State1} ->
            report_connection_status(connection_up, State1),
            {noreply, send_waiting(State1#state{connection_state = up,
                                                pending_timer = none})}
    end;



handle_info({connected, _Pid}, State) ->
    {noreply, init_connection(State)};

handle_info({connect_error, _Pid, Reason}, State) ->
    {noreply, connection_error({connect_error, Reason}, State)};

handle_info({socket_closed, _Pid, Reason}, State) ->
    {noreply, connection_error({socket_closed, Reason}, State)};

handle_info(do_reconnect, State) ->
    {noreply, start_connect(State)};

handle_info({timeout, TimerRef, pending}, State) when TimerRef == State#state.pending_timer ->
    State1 = reply_all({error, node_down}, State),
    {noreply, State1#state{connection_state = down}};

handle_info({timeout, TimerRef, init}, State) when TimerRef == State#state.init_timer ->
    {noreply, init_connection(State)};

handle_info({timeout, _TimerRef, _Msg}, State) ->
    {noreply, State}.



terminate(Reason, State) ->
    %% This could be done more gracefully by killing the connection process if up
    %% and waiting for trailing request replies and incoming requests. This would
    %% mean introducing a separate stop function and a stopped state.
    %% For now just cancel all requests and die
    reply_all({error, {client_stopped, Reason}}, State),
    report_connection_status({connection_down, {client_stopped, Reason}}, State),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

format_status(_Opt, Status) ->
    Status.

%%%===================================================================
%%% Internal functions
%%%===================================================================
reply_all(Reply, State = #state{waiting = Waiting, pending = Pending}) ->
    [reply_request(Request, Reply) || Request <- q_to_list(Pending)],
    [reply_request(Request, Reply) || Request <- q_to_list(Waiting)],
    State#state{waiting = q_clear(Waiting), pending = q_clear(Pending)}.


start_connect(State) ->
    #state{host = Host, port = Port, connection_opts = Opts} = State,
    Pid = redis_connection:connect_async(Host, Port, Opts),
    State#state{connection_pid = Pid}.

init_connection(State = #state{resp_version = Resp, use_cluster_id = UseClusterId}) ->
    Commands = [[<<"HELLO">>, <<"3">>] || Resp == 3] ++ [[<<"CLUSTER">>, <<"MYID">>] || UseClusterId],
    %io:format("~p\n", [Commands]),
    case Commands of
        [] ->
            self() ! {init_request_reply, []};
        _ ->
            redis_connection:request_async(State#state.connection_pid, Commands, init_request_reply)
    end,
    State.

%% init_connection(State) ->
%%     case State#state.resp_version of
%%         3 ->
%%             redis_connection:request(State#state.connection_pid, [<<"hello">>, <<"3">>], State#state.pending_timeout);
%%         2 ->
%%             ok
%%     end.

init_error(Reason, State) ->
    case State#state.connection_state of
        S when S == down; S == pending ->
            State#state{init_timer = erlang:start_timer(State#state.reconnect_wait, self(), init)};
        _ ->
            report_connection_status({connection_down, Reason}, State),
            State2 = cancel_pending_requests(State),
            State2#state{connection_state = pending,
                         pending_timer = erlang:start_timer(State#state.pending_timeout, self(), pending),
                         init_timer = erlang:start_timer(State#state.reconnect_wait, self(), init)}
    end.



connection_error(Reason, State) ->
    case State#state.connection_state of
        S when S == down; S == pending ->
            erlang:send_after(State#state.reconnect_wait, self(), do_reconnect),
            State;
        _ -> %% initial, up
            report_connection_status({connection_down, Reason}, State),
            State2 = cancel_pending_requests(State),
            start_connect(State2#state{connection_state = pending,
                                       pending_timer = erlang:start_timer(State#state.pending_timeout, self(), pending),
                                       init_timer = none % no need for this until connection is up again
                                      })
    end.


cancel_pending_requests(State) ->
    case q_out(State#state.pending) of
        empty ->
            State;
         {Request, OtherPending} ->
            case q_in_first(Request, State#state.waiting) of
                full ->
                    reply_request(Request, {error, queue_overflow}),
                    cancel_pending_requests(State#state{pending = OtherPending});
                NewWaiting ->
                    cancel_pending_requests(State#state{pending = OtherPending, waiting = NewWaiting})
            end
    end.


%%%%%%
new_request(Request, #state{connection_state = down}) ->
    reply_request(Request, {error, node_down});

new_request(Request, State = #state{connection_state = up}) ->
    #state{waiting = Waiting, pending = Pending, connection_pid = Conn} = State,
    case send_request(Request, Pending, Conn) of
        full ->
            State#state{waiting = do_wait(Request, Waiting)};
        Q ->
            State#state{pending = Q}
    end;
new_request(Request, State = #state{waiting = Waiting}) ->
    State#state{waiting = do_wait(Request, Waiting)}.


send_request(Request, Pending, Conn) ->
    case q_in(Request, Pending) of
        full ->
            full;
        Q ->
            Data = get_request_payload(Request),
            redis_connection:request_async(Conn, Data, request_reply),
            Q
    end.

do_wait(Request, Waiting) ->
    case q_in(Request, Waiting) of
        full ->
            % If queue is full kick out the one in front. Might not seem fair but
            % propably the one in front is tired of queueing and the call might even
            % have timed out  already
            {OldRequest, Q} = q_out(Waiting),
            reply_request(OldRequest, {error, queue_overflow}),
            q_in(Request, Q);
        Q ->
            Q
    end.

send_waiting(State = #state{waiting = Waiting, pending = Pending, connection_pid = Conn}) ->
    case q_out(Waiting) of
        empty ->
            State;
        {Request, NewWaiting} ->
            case send_request(Request, Pending, Conn) of
                full ->
                    State;
                NewPending ->
                    send_waiting(State#state{waiting = NewWaiting, pending = NewPending})
            end
    end.

q_new(Max) ->
    {0, Max, queue:new()}.

q_in(Item, {Size, Max, Q}) ->
    case Size >= Max of
        true -> full;
        false -> {Size+1, Max, queue:in(Item, Q)}
    end.

q_in_first(Item, {Size, Max, Q}) ->
    case Size >= Max of
        true -> full;
        false -> {Size+1, Max, queue:in_r(Item, Q)}
    end.

q_out({Size, Max, Q}) ->
    case queue:out(Q) of
        {empty, _Q} -> empty;
        {{value, Val}, NewQ} -> {Val, {Size-1, Max, NewQ}}
    end.

q_out_last({Size, Max, Q}) ->
    case queue:out_r(Q) of
        {empty, _Q} -> empty;
        {{value, Val}, NewQ} -> {Val, {Size-1, Max, NewQ}}
    end.

q_to_list({_Size, _Max, Q}) ->
    queue:to_list(Q).

q_clear({_, Max, _}) ->
    q_new(Max).

reply_request({request, _, Fun}, Reply) ->
    Fun(Reply).

get_request_payload({request, Request, _Fun}) ->
    Request.

%% report_connection_state_info(State) ->

report_connection_status(Status, State = #state{host = Host, port = Port, cluster_id = ClusterId}) ->
    Msg = {connection_status, {self(), {Host, Port}, ClusterId}, Status},
    send_info(Msg, State).


-spec send_info(info_msg(), #state{}) -> ok.
send_info(Msg, #state{info_pid = Pid}) ->
    case Pid of
        none -> ok;
        _ -> Pid ! Msg % TODO add more info
    end.
