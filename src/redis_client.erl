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
                down_timeout = 3000 :: non_neg_integer(),
                use_cluster_id = false :: boolean(),
                queue_ok_level = 2000,
                connection_pid = none,
                last_status = none,

                waiting = q_new(5000),
                pending = q_new(128),
                connection_state = initial, % initial, up, down, pending
                cluster_id = undefined :: undefined | binary(),
                queue_full = false :: boolean(), % set to true when full, false when reaching queue_ok_level

                pending_timer = none :: none | reference(), %% Timer to go from pending to down
                init_timer = none :: none | reference() %% Timer to retry the init procedure
               }
       ).

-type host()        :: inet:socket_address() | inet:hostname().
-type addr()        :: {host(), inet:port_number()}.
-type node_id()     :: binary() | undefined.
-type client_info() :: {pid(), addr(), node_id()}.
-type status()      :: connection_up | {connection_down, down_reason()} | queue_ok | queue_full.
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
                 ({queue_ok_level, Val}, S)  -> S#state{queue_ok_level = Val};
                 ({reconnect_wait, Val}, S)  -> S#state{reconnect_wait = Val};
                 ({info_pid, Val}, S)        -> S#state{info_pid = Val};
                 ({resp_version, Val}, S)    -> S#state{resp_version = Val};
                 ({down_timeout, Val}, S) -> S#state{down_timeout = Val};
                 ({use_cluster_id, Val}, S)  -> S#state{use_cluster_id = Val};
                 (Other, _)                  -> error({badarg, Other})
              end,
              #state{host = Host, port = Port},
              Opts),
    {ok, start_connect(State)}.

handle_call({request, Request}, From, State) ->
    Fun = fun(Reply) -> gen_server:reply(From, Reply) end,
    handle_cast({request, Request, Fun}, State).

-spec handle_cast(any(), #state{}) -> {noreply, #state{}}.

handle_cast(Request, State) ->
    {noreply, new_request(Request, State)}.

-spec handle_info(any(), #state{}) -> {noreply, #state{}}.

handle_info({{request_reply, Pid}, Reply}, State = #state{pending = Pending, connection_pid = Pid}) ->
    case q_out(Pending) of
        empty ->
            {noreply, State};
        {Request, NewPending} ->
            reply_request(Request, {ok, Reply}),
            {noreply, send_waiting(State#state{pending = NewPending})}
    end;

handle_info({request_reply, _Pid, _Reply}, State) ->
    %% Stray message from a defunct client? ignore!
    {noreply, State};

handle_info({connected, _Pid}, State) ->
    {noreply, init_connection(State)};

handle_info({connect_error, _Pid, Reason}, State) ->
    %% wait a bit until next reconnect attempt
    erlang:send_after(State#state.reconnect_wait, self(), do_reconnect),
    {noreply, connection_pending({connect_error, Reason}, State)};

handle_info({socket_closed, _Pid, Reason}, State) ->
    State1 = connection_pending({socket_closed, Reason}, State),
    %% cancel init timer since connection is down and try to reconnect immediately
    {noreply, start_connect(State1#state{init_timer = none})};

handle_info({init_request_reply, Reply}, State) ->
    case [Reason || {error, Reason} <- Reply] of
        [] ->
            State1 = case Reply of
                         [_Hello, ClusterId] ->
                             State#state{cluster_id = ClusterId};
                         [ClusterId] when State#state.use_cluster_id ->
                             State#state{cluster_id = ClusterId};
                         _Other -> %% Hello or empty
                             State
                 end,
            State2 = report_connection_status(connection_up, State1),
            {noreply, send_waiting(State2#state{connection_state = up,
                                                pending_timer = none})};
        Error ->
            %% start the init timer to resend the initial messages
            Tref = erlang:start_timer(State#state.reconnect_wait, self(), init),
            {noreply, connection_pending({init_error, Error}, State#state{init_timer = Tref})}
    end;

handle_info(do_reconnect, State) ->
    {noreply, start_connect(State)};

handle_info({timeout, TimerRef, pending}, State) when TimerRef == State#state.pending_timer ->
    State1 = reply_all({error, node_down}, State),
    State2 = if
                 State1#state.queue_full ->
                     report_connection_status(queue_ok, State1);
                 true ->
                     State1
             end,
    {noreply, State2#state{connection_state = down, queue_full = false}};

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
     case Commands of
        [] ->
            self() ! {init_request_reply, []};
        _ ->
            redis_connection:request_async(State#state.connection_pid, Commands, init_request_reply)
    end,
    State.

connection_pending(Reason, State) ->
    State1 = report_connection_status({connection_down, Reason}, State),
    case lists:member(State#state.connection_state, [initial,up]) of
        true ->
            Tref = erlang:start_timer(State1#state.down_timeout, self(), pending),
            cancel_pending_requests(State1#state{connection_state = pending, pending_timer = Tref});
        false ->
            State1
    end.

cancel_pending_requests(State) ->
    case q_out(State#state.pending) of
        empty ->
            State;
         {Request, OtherPending} ->
            case q_in_first(Request, State#state.waiting) of
                full when State#state.queue_full ->
                    reply_request(Request, {error, queue_overflow}),
                    cancel_pending_requests(State#state{pending = OtherPending});
                full ->
                    report_connection_status(queue_full, State),
                    reply_request(Request, {error, queue_overflow}),
                    cancel_pending_requests(State#state{pending = OtherPending, queue_full = true});
                NewWaiting ->
                    cancel_pending_requests(State#state{pending = OtherPending, waiting = NewWaiting})
            end
    end.


%%%%%%
-spec new_request(any(), #state{}) -> #state{}.

new_request(Request, State = #state{connection_state = down}) ->
    reply_request(Request, {error, node_down}),
    State;

new_request(Request, State = #state{connection_state = up}) ->
    #state{pending = Pending, connection_pid = Conn} = State,
    case send_request(Request, Pending, Conn) of
        full ->
            do_wait(Request, State);
        Q ->
            State#state{pending = Q}
    end;
new_request(Request, State) ->
    do_wait(Request, State).


send_request(Request, Pending, Conn) ->
    case q_in(Request, Pending) of
        full ->
            full;
        Q ->
            Data = get_request_payload(Request),
            redis_connection:request_async(Conn, Data, {request_reply, Conn}),
            Q
    end.

-spec do_wait(any(), #state{}) -> #state{}.

do_wait(Request, State = #state{waiting = Waiting}) ->
    case q_in(Request, Waiting) of
        full ->
            % If queue is full kick out the one in front. Might not seem fair but
            % propably the one in front is tired of queueing and the call might even
            % have timed out  already
            {OldRequest, Q} = q_out(Waiting),
            reply_request(OldRequest, {error, queue_overflow}),
            State1 = if
                         not State#state.queue_full ->
                             report_connection_status(queue_full, State);
                         true ->
                             State
                     end,
            State1#state{waiting = q_in(Request, Q), queue_full = true};
        Q ->
            State#state{waiting = Q}
    end.

-spec send_waiting(#state{}) -> #state{}.

send_waiting(State = #state{waiting = Waiting, pending = Pending, connection_pid = Conn}) ->
    case q_out(Waiting) of
        empty ->
            State;
        {Request, NewWaiting} ->
            case send_request(Request, Pending, Conn) of
                full ->
                    State;
                NewPending ->
                    %% check if queue was full and is now on a OK level
                    case State#state.queue_full andalso (q_len(NewWaiting) =< State#state.queue_ok_level) of
                        true ->
                            State2 = report_connection_status(queue_ok, State),
                            send_waiting(State2#state{waiting = NewWaiting, pending = NewPending, queue_full = false});
                        false ->
                            send_waiting(State#state{waiting = NewWaiting, pending = NewPending})
                    end
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

%% q_out_last({Size, Max, Q}) ->
%%     case queue:out_r(Q) of
%%         {empty, _Q} -> empty;
%%         {{value, Val}, NewQ} -> {Val, {Size-1, Max, NewQ}}
%%     end.

q_to_list({_Size, _Max, Q}) ->
    queue:to_list(Q).

q_clear({_, Max, _}) ->
    q_new(Max).

q_len({Size, _Max, _Q}) ->
    Size.

reply_request({request, _, Fun}, Reply) ->
    Fun(Reply).

get_request_payload({request, Request, _Fun}) ->
    Request.

%% report_connection_state_info(State) ->

report_connection_status(Status, State = #state{last_status = Status}) ->
    State;
report_connection_status(Status, State = #state{host = Host, port = Port, cluster_id = ClusterId}) ->
    Msg = {connection_status, {self(), {Host, Port}, ClusterId}, Status},
    send_info(Msg, State),
    State#state{last_status = Status}.


-spec send_info(info_msg(), #state{}) -> ok.
send_info(Msg, #state{info_pid = Pid}) ->
    case Pid of
        none -> ok;
        _ -> Pid ! Msg % TODO add more info
    end.
