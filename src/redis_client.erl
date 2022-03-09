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


%-record(st, {}).
-record(opts,
        {
         host                   :: host(),
         port                   :: inet:port_number(),
         connection_opts = []   :: [redis_connection:opts()],
         resp_version = 3       :: 2..3,
         use_cluster_id = false :: boolean(),
         reconnect_wait = 1000  :: non_neg_integer(),
         connect_timeout = 3000 :: non_neg_integer(),

         queue_timeout = 3000   :: non_neg_integer(),
         info_pid = none        :: none | pid(),
         queue_ok_level = 2000  :: non_neg_integer(),

         max_waiting = 5000     :: non_neg_integer(),
         max_pending = 128      :: non_neg_integer()
        }).

-type request_reply()    :: {ok, redis_connection:result()} | {error, request_error()}.
-type request_error()    :: queue_overflow | node_down | {client_stopped, reason()}.
-type request_callback() :: fun((request_reply()) -> any()).
-type request_item()     :: {request, redis_lib:request(), request_callback()}.
-type request_queue()    :: {Size :: non_neg_integer(), Max :: non_neg_integer(), queue:queue(request_item())}.



-record(state,
        {
         connection_state = pending :: pending | up | down,
         connection_pid = none,
         last_status = none,

         waiting :: undefined | request_queue(),
         pending :: undefined | request_queue(),

         cluster_id = undefined :: undefined | binary(),
         queue_full = false :: boolean(), % set to true when full, false when reaching queue_ok_level

         queue_timer = none :: none | reference(),
         opts :: undefined | #opts{}

        }).

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
init([Host, Port, OptsList]) ->
    Opts = lists:foldl(
             fun({connection_opts, Val}, S) -> S#opts{connection_opts = Val};
                ({max_waiting, Val}, S)     -> S#opts{max_waiting = Val};
                ({max_pending, Val}, S)     -> S#opts{max_pending = Val};
                ({queue_ok_level, Val}, S)  -> S#opts{queue_ok_level = Val};
                ({reconnect_wait, Val}, S)  -> S#opts{reconnect_wait = Val};
                ({info_pid, Val}, S)        -> S#opts{info_pid = Val};
                ({resp_version, Val}, S)    -> S#opts{resp_version = Val};
                ({connect_timeout, Val}, S) -> S#opts{connect_timeout = Val};
                ({queue_timeout, Val}, S)   -> S#opts{queue_timeout = Val};
                ({use_cluster_id, Val}, S)  -> S#opts{use_cluster_id = Val};
                (Other, _)                  -> error({badarg, Other})
             end,
             #opts{host = Host, port = Port},
             OptsList),

    Pid = self(),
    spawn_link(fun() -> connect(Pid, Opts) end),
    {ok, #state{opts = Opts,
                waiting = q_new(Opts#opts.max_waiting),
                pending = q_new(Opts#opts.max_pending)}}.

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


handle_info(Reason = connect_timeout, State) ->
    {noreply, connection_down({connection_down, Reason}, State#state{connection_state = down})};

handle_info(Reason = {connect_error, _ErrorReason}, State) ->
    {noreply, connection_down({connection_down, Reason}, State)};

handle_info(Reason = {socket_closed, _CloseReason}, State) ->
    {noreply, connection_down(Reason, State)};

handle_info(Reason = {init_error, _Errors}, State) ->
    {noreply, connection_down({connection_down, Reason}, State)};

handle_info({connected, Pid, ClusterId}, State) ->
    State1 = State#state{connection_pid = Pid, cluster_id = ClusterId, queue_timer = none},
    State2 = report_connection_status(connection_up, State1),
    State3 = send_waiting(State2),
    {noreply, State3#state{connection_state = up}};

handle_info({timeout, TimerRef, queue}, State) when TimerRef == State#state.queue_timer ->
    State1 = reply_all({error, node_down}, State),
    State2 = if
                 State1#state.queue_full ->
                     report_connection_status(queue_ok, State1);
                 true ->
                     State1
             end,
    {noreply, State2#state{queue_full = false}};


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


connection_down(Reason, State) when State#state.connection_pid /= none ->
    Tref = erlang:start_timer(State#state.opts#opts.queue_timeout, self(), queue),
    connection_down(Reason, State#state{queue_timer = Tref, connection_pid = none});

connection_down(Reason, State = #state{connection_state = up}) ->
    connection_down(Reason, State#state{connection_state = pending});

connection_down(Reason, State) ->
    cancel_pending_requests(report_connection_status(Reason, State)).


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

new_request(Request, State) ->
    case State#state.connection_state of
        up ->
            case send_request(Request, State#state.pending, State#state.connection_pid) of
                full ->
                    do_wait(Request, State);
                Q ->
                    State#state{pending = Q}
            end;
        pending ->
            do_wait(Request, State);
        down ->
            reply_request(Request, {error, node_down}),
            State
    end.


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
                    QueueOkLevel = State#state.opts#opts.queue_ok_level,
                    case State#state.queue_full andalso (q_len(NewWaiting) =< QueueOkLevel) of
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
report_connection_status(Status, State) ->
    #opts{host = Host, port = Port} = State#state.opts,
    ClusterId = State#state.cluster_id,
    Msg = {connection_status, {self(), {Host, Port}, ClusterId}, Status},
    send_info(Msg, State),
    State#state{last_status = Status}.


-spec send_info(info_msg(), #state{}) -> ok.
send_info(Msg, State) ->
    Pid = State#state.opts#opts.info_pid,
    case Pid of
        none -> ok;
        _ -> Pid ! Msg % TODO add more info
    end.


connect(Pid, Opts) -> % Host, Port, Opts, ReconnectWait, ConnectTimeout) ->
    TRef = erlang:send_after(Opts#opts.connect_timeout, Pid, connect_timeout),
    Result = redis_connection:connect(Opts#opts.host, Opts#opts.port, Opts#opts.connection_opts),
    erlang:cancel_timer(TRef),
    case Result of
        {error, Reason} ->
            Pid ! {connect_error, Reason},
            timer:sleep(Opts#opts.reconnect_wait);

        {ok, ConnectionPid} ->
            case init(Pid, ConnectionPid, Opts) of
                {socket_closed, ConnectionPid, Reason} ->
                    Pid ! {socket_closed, Reason};
                {ok, ClusterId}  ->
                    Pid ! {connected, ConnectionPid, ClusterId},
                    receive
                        {socket_closed, ConnectionPid, Reason} ->
                            Pid ! {socket_closed, Reason}
                    end
            end

    end,
    connect(Pid, Opts).


init(MainPid, ConnectionPid, Opts) ->
    Cmd1 =  [[<<"CLUSTER">>, <<"MYID">>] || Opts#opts.use_cluster_id],
    Cmd2 =  [[<<"HELLO">>, <<"3">>] || Opts#opts.resp_version == 3],
    case Cmd1 ++ Cmd2 of
        [] ->
            {ok, undefined};
        Commands ->
            redis_connection:request_async(ConnectionPid, Commands, init_request_reply),
            receive
                {init_request_reply, Reply} ->
                    case [Reason || {error, Reason} <- Reply] of
                        [] when Opts#opts.use_cluster_id ->
                            {ok, hd(Reply)};
                        []  ->
                            {ok, undefined};
                        Errors ->
                            MainPid ! {init_error, Errors},
                            timer:sleep(Opts#opts.reconnect_wait),
                            init(MainPid, ConnectionPid, Opts)
                    end;
                Other ->
                    Other
            end
    end.
