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
-type request_queue()    :: {Size :: non_neg_integer(), queue:queue(request_item())}.



-record(st,
        {
         connection_pid = none,
         last_status = none,

         waiting = q_new() :: request_queue(),
         pending = q_new() :: request_queue(),

         cluster_id = undefined :: undefined | binary(),

         queue_full_event_sent = false :: boolean(), % set to true when full, false when reaching queue_ok_level
         node_down = false :: boolean(),

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
                ({queue_timeout, Val}, S)   -> S#opts{queue_timeout = Val};
                ({use_cluster_id, Val}, S)  -> S#opts{use_cluster_id = Val};
                (Other, _)                  -> error({badarg, Other})
             end,
             #opts{host = Host, port = Port},
             OptsList),

    Pid = self(),
    spawn_link(fun() -> connect(Pid, Opts) end),
%    {ok, #st{opts = Opts}}.
    {ok, start_node_down_timer(#st{opts = Opts})}.

handle_call({request, Request}, From, State) ->
    Fun = fun(Reply) -> gen_server:reply(From, Reply) end,
    handle_cast({request, Request, Fun}, State).


handle_cast(Request, State) ->
    if
        State#st.node_down ->
            {noreply, reply_request(Request, {error, node_down})};
        true ->
            {noreply, process_requests(State#st{waiting = q_in(Request, State#st.waiting)})}
    end.


handle_info({{request_reply, Pid}, Reply}, State = #st{pending = Pending, connection_pid = Pid}) ->
    case q_out(Pending) of
        empty ->
            {noreply, State};
        {Request, NewPending} ->
            reply_request(Request, {ok, Reply}),
            {noreply, process_requests(State#st{pending = NewPending})}
    end;

handle_info({request_reply, _Pid, _Reply}, State) ->
    %% Stray message from a defunct client? ignore!
    {noreply, State};

handle_info(Reason = {connect_error, _ErrorReason}, State) ->
    {noreply, connection_down({connection_down, Reason}, State)};

handle_info(Reason = {socket_closed, _CloseReason}, State) ->
    {noreply, connection_down(Reason, State)};

handle_info(Reason = {init_error, _Errors}, State) ->
    {noreply, connection_down({connection_down, Reason}, State)};

handle_info({connected, Pid, ClusterId}, State) ->
    erlang:cancel_timer(State#st.queue_timer),
    State1 = State#st{connection_pid = Pid, cluster_id = ClusterId, queue_timer = none},
    State2 = report_connection_status(connection_up, State1),
    {noreply, process_requests(State2#st{node_down = false})};

handle_info({timeout, TimerRef, queue}, State) when TimerRef == State#st.queue_timer ->
    State1 = reply_all({error, node_down}, State),
    {noreply, process_requests(State1#st{node_down = true})};


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
reply_all(Reply, State = #st{waiting = Waiting, pending = Pending}) ->
    [reply_request(Request, Reply) || Request <- q_to_list(Pending)],
    [reply_request(Request, Reply) || Request <- q_to_list(Waiting)],
    State#st{waiting = q_new(), pending = q_new()}.


start_node_down_timer(State) ->
    case State#st.queue_timer of
        none ->
            State#st{queue_timer = erlang:start_timer(State#st.opts#opts.queue_timeout, self(), queue)};
        _ ->
            State
    end.

%% stop_node_down_timer(State) ->
%%     case State#st.queue_timer of
%%         none ->
%%             State;
%%         Tref ->
%%             erlang:stop_timer(Tref),
%%             State#st{queue_timer = none};
%%          end.

connection_down(Reason, State) ->
    State1 = State#st{waiting = q_join(State#st.pending, State#st.waiting),
                      pending = q_new(),
                      connection_pid = none},
    State2 = process_requests(State1),
    State3 = report_connection_status(Reason, State2),
    start_node_down_timer(State3).


%%%%%%
process_requests(State) ->
    NumWaiting = q_len(State#st.waiting),
    NumPending = q_len(State#st.pending),
    if
        (NumWaiting > 0) and (NumPending < State#st.opts#opts.max_pending) and (State#st.connection_pid /= none) ->
            {Request, NewWaiting} = q_out(State#st.waiting),
            Data = get_request_payload(Request),
            redis_connection:request_async(State#st.connection_pid, Data, {request_reply, State#st.connection_pid}),
            process_requests(State#st{pending = q_in(Request, State#st.pending),
                                      waiting = NewWaiting});

        (NumWaiting > State#st.opts#opts.max_waiting) and (State#st.queue_full_event_sent) ->
            drop_requests(State);

        NumWaiting > State#st.opts#opts.max_waiting ->
            drop_requests(
              report_connection_status(queue_full, State#st{queue_full_event_sent = true}));

        (NumWaiting < State#st.opts#opts.queue_ok_level) and (State#st.queue_full_event_sent) ->
            report_connection_status(queue_ok, State#st{queue_full_event_sent = false});

        true ->
            State
    end.

drop_requests(State) ->
    case q_len(State#st.waiting) > State#st.opts#opts.max_waiting of
        true ->
            {OldRequest, NewWaiting} = q_out(State#st.waiting),
            reply_request(OldRequest, {error, queue_overflow}),
            drop_requests(State#st{waiting = NewWaiting});
        false  ->
            State
    end.

q_new() ->
    {0, queue:new()}.

q_in(Item, {Size, Q}) ->
    {Size+1, queue:in(Item, Q)}.

q_join({Size1, Q1}, {Size2, Q2}) ->
    {Size1 + Size2, queue:join(Q1, Q2)}.

q_out({Size, Q}) ->
    case queue:out(Q) of
        {empty, _Q} -> empty;
        {{value, Val}, NewQ} -> {Val, {Size-1, NewQ}}
    end.

q_to_list({_Size, Q}) ->
    queue:to_list(Q).

q_len({Size, _Q}) ->
    Size.


reply_request({request, _, Fun}, Reply) ->
    Fun(Reply).

get_request_payload({request, Request, _Fun}) ->
    Request.

report_connection_status(Status, State = #st{last_status = Status}) ->
    State;
report_connection_status(Status, State) ->
    #opts{host = Host, port = Port} = State#st.opts,
    ClusterId = State#st.cluster_id,
    Msg = {connection_status, {self(), {Host, Port}, ClusterId}, Status},
    send_info(Msg, State),
    State#st{last_status = Status}.


-spec send_info(info_msg(), #st{}) -> ok.
send_info(Msg, State) ->
    Pid = State#st.opts#opts.info_pid,
    case Pid of
        none -> ok;
        _ -> Pid ! Msg % TODO add more info
    end.


connect(Pid, Opts) -> % Host, Port, Opts, ReconnectWait, ConnectTimeout) ->
    Result = redis_connection:connect(Opts#opts.host, Opts#opts.port, Opts#opts.connection_opts),
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
