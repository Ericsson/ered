-module(ered_client).

%% Queues messages for a specific node. Manages reconnects and resends
%% in case of error. Reports connection status with status messages.
%% This is implemented as one gen_server for message queue and a
%% separate process to handle reconnects.

-behaviour(gen_server).

%% API

-export([start_link/3,
         stop/1, deactivate/1, reactivate/1,
         command/2, command/3,
         command_async/3]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-export_type([info_msg/0,
              addr/0,
              server_ref/0,
              opt/0,
              reply/0,
              reply_fun/0
             ]).

%%%===================================================================
%%% Definitions
%%%===================================================================

-record(opts,
        {
         host :: host(),
         port :: inet:port_number(),
         connection_opts = [] :: [ered_connection:opt()],
         resp_version = 3 :: 2..3,
         use_cluster_id = false :: boolean(),
         auth = none :: {binary(), binary()} | none,
         reconnect_wait = 1000 :: non_neg_integer(),

         node_down_timeout = 2000 :: non_neg_integer(),
         info_pid = none :: none | pid(),
         queue_ok_level = 2000 :: non_neg_integer(),

         max_waiting = 5000 :: non_neg_integer(),
         max_pending = 128 :: non_neg_integer()
        }).

-record(st,
        {
         connect_loop_pid = none,
         connection_pid = none,
         last_status = none,

         waiting = q_new() :: command_queue(),
         pending = q_new() :: command_queue(),

         cluster_id = undefined :: undefined | binary(),

         queue_full_event_sent = false :: boolean(), % set to true when full, false when reaching queue_ok_level
         node_status = up :: up | node_down | node_deactivated,

         node_down_timer = none :: none | reference(),
         opts = #opts{}

        }).


-type command_error()          :: queue_overflow | node_down | node_deactivated | {client_stopped, reason()}.
-type command_item()           :: {command, ered_command:redis_command(), reply_fun()}.
-type command_queue()          :: {Size :: non_neg_integer(), queue:queue(command_item())}.

-type reply()       :: {ok, ered_connection:result()} | {error, command_error()}.
-type reply_fun()   :: fun((reply()) -> any()).

-type host()        :: ered_connection:host().
-type addr()        :: {host(), inet:port_number()}.
-type node_id()     :: binary() | undefined.
-type client_info() :: {pid(), addr(), node_id()}.
-type status()      :: connection_up | {connection_down, down_reason()} | node_deactivated |
                       queue_ok | queue_full.
-type reason()      :: term(). % ssl reasons are of type any so no point being more specific
-type down_reason() :: node_down_timeout |
                       {client_stopped | connect_error | init_error | socket_closed,
                        reason()}.
-type info_msg()    :: {connection_status, client_info(), status()}.
-type server_ref()  :: pid().

-type opt() ::
        %% Options passed to the connection module
        {connection_opts, [ered_connection:opt()]} |
        %% Max number of commands allowed to wait in queue.
        {max_waiting, non_neg_integer()} |
        %% Max number of commands to be pending, i.e. sent to client
        %% and waiting for a response.
        {max_pending, non_neg_integer()} |
        %% If the queue has been full then it is considered ok
        %% again when it reaches this level
        {queue_ok_level, non_neg_integer()} |
        %% How long to wait to reconnect after a failed connect attempt
        {reconnect_wait, non_neg_integer()} |
        %% Pid to send status messages to
        {info_pid, none | pid()} |
        %% What RESP (REdis Serialization Protocol) version to use
        {resp_version, 2..3} |
        %% If there is a connection problem and the connection is
        %% not recovered before this timeout then the client considers
        %% the node down and will clear it's queue and reject all new
        %% commands until connection is restored.
        {node_down_timeout, non_neg_integer()} |
        %% Set if the CLUSTER ID should be fetched used in info messages.
        %% (not useful if the client is used outside of a cluster)
        {use_cluster_id, boolean()} |
        %% Username and password for Redis authentication (AUTH or HELLO).
        {auth, {binary(), binary()}}.

%%%===================================================================
%%% API
%%%===================================================================

%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-spec start_link(host(), inet:port_number(), [opt()]) ->
          {ok, server_ref()} | {error, term()}.
%%
%% Start the client process. Create a connection towards the provided
%% address.
%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
start_link(Host, Port, Opts) ->
    gen_server:start_link(?MODULE, [Host, Port, Opts], []).

%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-spec stop(server_ref()) -> ok.
%%
%% Stop the client process. Cancel all commands in queue. Take down
%% connection.
%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
stop(ServerRef) ->
    gen_server:stop(ServerRef).

%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-spec deactivate(server_ref()) -> ok.
%%
%% Prepares the client to stop. Cancel all commands in queue and put
%% the client in a 'node_deactivated' state. The client is still
%% running though, and can be reactivated if the node comes back to the
%% cluster before the client is stopped.
%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
deactivate(ServerRef) ->
    gen_server:cast(ServerRef, deactivate).

%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-spec reactivate(server_ref()) -> ok.
%%
%% Reactivates a client that has previously been deactivated. This is
%% done when a node comes back to a cluster before the client for that
%% node has been stopped.
%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
reactivate(ServerRef) ->
    gen_server:cast(ServerRef, reactivate).

%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-spec command(server_ref(), ered_command:command()) -> reply().
-spec command(server_ref(), ered_command:command(), timeout()) -> reply().
%%
%% Send a command to the connected Redis node. The argument can be a
%% single command as a list of binaries, a pipeline of command as a
%% list of commands or a formatted redis_command.
%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
command(ServerRef, Command) ->
    command(ServerRef, Command, infinity).

command(ServerRef, Command, Timeout) ->
    gen_server:call(ServerRef, {command, ered_command:convert_to(Command)}, Timeout).

%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-spec command_async(server_ref(), ered_command:command(), reply_fun()) -> ok.
%%
%% Send a command to the connected Redis node in asynchronous
%% fashion. The provided callback function will be called with the
%% reply. Note that the callback function will executing in the redis
%% client process and should not hang or perform any lengthy task.
%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
command_async(ServerRef, Command, CallbackFun) ->
    gen_server:cast(ServerRef, {command, ered_command:convert_to(Command), CallbackFun}).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================
init([Host, Port, OptsList]) ->
    Opts = lists:foldl(
             fun({connection_opts, Val}, S)   -> S#opts{connection_opts = Val};
                ({max_waiting, Val}, S)       -> S#opts{max_waiting = Val};
                ({max_pending, Val}, S)       -> S#opts{max_pending = Val};
                ({queue_ok_level, Val}, S)    -> S#opts{queue_ok_level = Val};
                ({reconnect_wait, Val}, S)    -> S#opts{reconnect_wait = Val};
                ({info_pid, Val}, S)          -> S#opts{info_pid = Val};
                ({resp_version, Val}, S)      -> S#opts{resp_version = Val};
                ({node_down_timeout, Val}, S) -> S#opts{node_down_timeout = Val};
                ({use_cluster_id, Val}, S)    -> S#opts{use_cluster_id = Val};
                ({auth, Auth = {_, _}}, S)    -> S#opts{auth = Auth};
                (Other, _)                    -> error({badarg, Other})
             end,
             #opts{host = Host, port = Port},
             OptsList),

    process_flag(trap_exit, true),
    Pid = self(),
    ConnectPid = spawn_link(fun() -> connect(Pid, Opts) end),
    {ok, start_node_down_timer(#st{opts = Opts,
                                   connect_loop_pid = ConnectPid})}.

handle_call({command, Command}, From, State) ->
    Fun = fun(Reply) -> gen_server:reply(From, Reply) end,
    handle_cast({command, Command, Fun}, State).


handle_cast(Command = {command, _, _}, State) ->
    case State#st.node_status of
        up ->
            {noreply, process_commands(State#st{waiting = q_in(Command, State#st.waiting)})};
        NodeProblem when NodeProblem =:= node_down; NodeProblem =:= node_deactivated ->
            reply_command(Command, {error, NodeProblem}),
            {noreply, State}
    end;

handle_cast(deactivate, State) ->
    State1 = cancel_node_down_timer(State),
    State2 = report_connection_status(node_deactivated, State1),
    State3 = reply_all({error, node_deactivated}, State2),
    {noreply, process_commands(State3#st{node_status = node_deactivated})};

handle_cast(reactivate, #st{connection_pid = none} = State) ->
    {noreply, start_node_down_timer(State)};

handle_cast(reactivate, State) ->
    {noreply, State#st{node_status = up}}.


handle_info({{command_reply, Pid}, Reply}, State = #st{pending = Pending, connection_pid = Pid}) ->
    case q_out(Pending) of
        empty ->
            {noreply, State};
        {Command, NewPending} ->
            reply_command(Command, {ok, Reply}),
            {noreply, process_commands(State#st{pending = NewPending})}
    end;

handle_info({{command_reply, _Pid}, _Reply}, State) ->
    %% Stray message from a defunct client? ignore!
    {noreply, State};

handle_info(Reason = {connect_error, _ErrorReason}, State) ->
    {noreply, connection_down(Reason, State)};

handle_info(Reason = {init_error, _Errors}, State) ->
    {noreply, connection_down(Reason, State)};

handle_info(Reason = {socket_closed, _CloseReason}, State) ->
    {noreply, connection_down(Reason, State)};

handle_info({connected, Pid, ClusterId}, State) ->
    State1 = cancel_node_down_timer(State),
    State2 = State1#st{connection_pid = Pid, cluster_id = ClusterId,
                       node_status = case State1#st.node_status of
                                         node_down -> up;
                                         OldStatus -> OldStatus
                                     end},
    State3 = report_connection_status(connection_up, State2),
    {noreply, process_commands(State3)};

handle_info({timeout, TimerRef, node_down}, State) when TimerRef == State#st.node_down_timer ->
    State1 = report_connection_status({connection_down, node_down_timeout}, State),
    State2 = reply_all({error, node_down}, State1),
    {noreply, process_commands(State2#st{node_status = node_down})};

handle_info({timeout, _TimerRef, _Msg}, State) ->
    {noreply, State};

handle_info({'EXIT', _From, Reason}, State) ->
    {stop, Reason, State}.

terminate(Reason, State) ->
    exit(State#st.connect_loop_pid, kill),
    reply_all({error, {client_stopped, Reason}}, State),
    report_connection_status({connection_down, {client_stopped, Reason}}, State),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
reply_all(Reply, State) ->
    [reply_command(Command, Reply) || Command <- q_to_list(State#st.pending)],
    [reply_command(Command, Reply) || Command <- q_to_list(State#st.waiting)],
    State#st{waiting = q_new(), pending = q_new()}.

start_node_down_timer(#st{node_down_timer = none} = State) ->
    Timeout = State#st.opts#opts.node_down_timeout,
    State#st{node_down_timer = erlang:start_timer(Timeout, self(), node_down)};
start_node_down_timer(State) ->
    State.

cancel_node_down_timer(#st{node_down_timer = none} = State) ->
    State;
cancel_node_down_timer(#st{node_down_timer = TimerRef} = State) ->
    erlang:cancel_timer(TimerRef),
    State#st{node_down_timer = none}.

connection_down(Reason, State) ->
    State1 = State#st{waiting = q_join(State#st.pending, State#st.waiting),
                      pending = q_new(),
                      connection_pid = none},
    State2 = process_commands(State1),
    State3 = report_connection_status({connection_down, Reason}, State2),
    start_node_down_timer(State3).


%%%%%%
process_commands(State) ->
    NumWaiting = q_len(State#st.waiting),
    NumPending = q_len(State#st.pending),
    if
        (NumWaiting > 0) and (NumPending < State#st.opts#opts.max_pending) and (State#st.connection_pid /= none) ->
            {Command, NewWaiting} = q_out(State#st.waiting),
            Data = get_command_payload(Command),
            ered_connection:command_async(State#st.connection_pid, Data, {command_reply, State#st.connection_pid}),
            process_commands(State#st{pending = q_in(Command, State#st.pending),
                                      waiting = NewWaiting});

        (NumWaiting > State#st.opts#opts.max_waiting) and (State#st.queue_full_event_sent) ->
            drop_commands(State);

        NumWaiting > State#st.opts#opts.max_waiting ->
            drop_commands(
              report_connection_status(queue_full, State#st{queue_full_event_sent = true}));

        (NumWaiting < State#st.opts#opts.queue_ok_level) and (State#st.queue_full_event_sent) ->
            report_connection_status(queue_ok, State#st{queue_full_event_sent = false});

        true ->
            State
    end.

drop_commands(State) ->
    case q_len(State#st.waiting) > State#st.opts#opts.max_waiting of
        true ->
            {OldCommand, NewWaiting} = q_out(State#st.waiting),
            reply_command(OldCommand, {error, queue_overflow}),
            drop_commands(State#st{waiting = NewWaiting});
        false  ->
            State
    end.

%% Some wrapper functions for queue + size for n(1) len checks
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


reply_command({command, _, Fun}, Reply) ->
    Fun(Reply).

get_command_payload({command, Command, _Fun}) ->
    Command.

report_connection_status(Status, State = #st{last_status = Status}) ->
    State;
report_connection_status(Status, State) ->
    #opts{host = Host, port = Port} = State#st.opts,
    ClusterId = State#st.cluster_id,
    Msg = {connection_status, {self(), {Host, Port}, ClusterId}, Status},
    send_info(Msg, State),
    case Status of
        %% Skip saving the last_status in this to avoid an extra connect_error event.
        %% The usual case is that there is a connect_error and then node_down and then
        %% more connect_errors..
        {connection_down, node_down_timeout} ->
            State;
        _ ->
            State#st{last_status = Status}
    end.


-spec send_info(info_msg(), #st{}) -> ok.
send_info(Msg, State) ->
    Pid = State#st.opts#opts.info_pid,
    case Pid of
        none ->
            ok;
        _ ->
            Pid ! Msg
    end,
    ok.


connect(Pid, Opts) ->
    Result = ered_connection:connect(Opts#opts.host, Opts#opts.port, Opts#opts.connection_opts),
    case Result of
        {error, Reason} ->
            Pid ! {connect_error, Reason},
            timer:sleep(Opts#opts.reconnect_wait);

        {ok, ConnectionPid} ->
            case init(Pid, ConnectionPid, Opts) of
                {socket_closed, ConnectionPid, Reason} ->
                    Pid ! {socket_closed, Reason},
                    timer:sleep(Opts#opts.reconnect_wait);
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
    Cmd2 = case {Opts#opts.resp_version, Opts#opts.auth} of
               {3, {Username, Password}} ->
                   [[<<"HELLO">>, <<"3">>, <<"AUTH">>, Username, Password]];
               {3, none} ->
                   [[<<"HELLO">>, <<"3">>]];
               {2, {Username, Password}} ->
                   [[<<"AUTH">>, Username, Password]];
               {2, none} ->
                   []
           end,
    case Cmd1 ++ Cmd2 of
        [] ->
            {ok, undefined};
        Commands ->
            ered_connection:command_async(ConnectionPid, Commands, init_command_reply),
            receive
                {init_command_reply, Reply} ->
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
