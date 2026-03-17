-module(ered_client).

%% Queues messages for a specific node. Manages reconnects and resends
%% in case of error. Reports connection status with status messages.
%% This is implemented as one gen_server for message queue and a
%% separate process to handle reconnects.

-behaviour(gen_server).

%% API

-export([start_link/3, start_link/4,
         connect/3, close/1,
         deactivate/1, reactivate/1,
         command/2, command/3,
         command_async/3]).

%% testing/debugging
-export([state_to_map/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-export_type([info_msg/0,
              addr/0,
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

         %% From "connection opts"
         batch_size = 16 :: pos_integer(),
         transport = gen_tcp :: gen_tcp | ssl,
         transport_opts = [] :: list(),
         connect_timeout = infinity :: timeout(),
         push_cb = fun(_) -> ok end :: push_cb(),
         timeout = 10000 :: timeout(), % Response timeout

         resp_version = 3 :: 2..3,
         use_cluster_id = false :: boolean(),
         auth = none :: {binary(), binary()} | none,
         select_db = 0 :: non_neg_integer(),
         reconnect_wait = 1000 :: non_neg_integer(),

         node_down_timeout = 2000 :: non_neg_integer(),
         info_pid = none :: none | pid(),
         queue_ok_level = 2000 :: non_neg_integer(),

         max_waiting = 5000 :: pos_integer(),
         max_pending = 128 :: pos_integer()
        }).

-record(st,
        {
         connection_loop_pid = none,
         socket = none,
         controlling_process :: pid(),
         last_status = none,
         parser_state :: ered_parser:state(),

         waiting = q_new() :: command_queue(),
         pending = q_new() :: command_queue(),

         %% Batching. When pending queue is full,
         %% set we don't send more until another
         %% complete batch can fit in the pending queue.
         filling_batch = true :: boolean(),

         cluster_id = undefined :: undefined | binary(),

         queue_full_event_sent = false :: boolean(), % set to true when full, false when reaching queue_ok_level
         status :: init | up | node_down | node_deactivated,
         node_down_timer = none :: none | reference(),
         connected_at = none :: none | integer(), % erlang:monotonic_time(millisecond)
         opts = #opts{}

        }).

-type command_error()          :: queue_overflow | node_down | node_deactivated | {client_stopped, reason()}.
-type command_item()           :: {command, ered_command:redis_command(), reply_fun()}.
-type command_queue()          :: {Size :: non_neg_integer(), queue:queue(command_item())}.

-type result()      :: ered_parser:parse_result().
-type push_cb()     :: fun((result()) -> any()).
-type reply()       :: {ok, result()} | {error, command_error()}.
-type reply_fun()   :: fun((reply()) -> any()).

-type host()        :: ered:host().
-type addr()        :: ered:addr().
-type status()      :: connection_up | {connection_down, down_reason()} | node_deactivated |
                       queue_ok | queue_full.
-type reason()      :: term(). % ssl reasons are of type any so no point being more specific
-type down_reason() :: node_down_timeout |
                       {client_stopped | connect_error | init_error | socket_closed,
                        reason()}.
-type info_msg(MsgType, Reason) ::
        #{msg_type := MsgType,
          reason := Reason,
          master => boolean(), % Optional. Added by ered_cluster.
          addr := addr(),
          client_id := pid(),
          cluster_id => binary() % Optional. Used by ered_cluster.
         }.
-type info_msg() ::
        info_msg(connected, none) |
        info_msg(socket_closed, any()) |
        info_msg(connect_error, any()) |
        info_msg(init_error, any()) |
        info_msg(node_down_timeout, none) |
        info_msg(node_deactivated, none) |
        info_msg(queue_ok, none) |
        info_msg(queue_full, none) |
        info_msg(client_stopped, any()).

-type opt() ::
        %% Options passed to the connection module
        {connection_opts, [connection_opt()]} |
        %% Max number of commands allowed to wait in queue.
        {max_waiting, pos_integer()} |
        %% Max number of commands to be pending, i.e. sent to client
        %% and waiting for a response.
        {max_pending, pos_integer()} |
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
        %% Username and password for authentication (AUTH or HELLO).
        {auth, {binary(), binary()}} |
        %% Select a logical database after a connect.
        %% The SELECT command is only sent when non-zero.
        {select_db, non_neg_integer()}.

-type connection_opt() ::
        %% If commands are queued up in the process message queue this is the max
        %% amount of messages that will be received and sent in one call
        {batch_size, pos_integer()} |
        %% Timeout passed to gen_tcp:connect/4 or ssl:connect/4.
        {connect_timeout, timeout()} |
        %% Options passed to gen_tcp:connect/4.
        {tcp_options, [gen_tcp:connect_option()]} |
        %% Timeout passed to gen_tcp:connect/4. DEPRECATED.
        {tcp_connect_timeout, timeout()} |
        %% Options passed to ssl:connect/4. If this config parameter is present,
        %% TLS is used.
        {tls_options, [ssl:tls_client_option()]} |
        %% Timeout passed to ssl:connect/4. DEPRECATED.
        {tls_connect_timeout, timeout()} |
        %% Callback for push notifications
        {push_cb, push_cb()} |
        %% Timeout when waiting for a response from Redis. milliseconds
        {response_timeout, non_neg_integer()}.

%% Command in the waiting queue.
-record(command, {data, replyto}).

%% Pending request, in flight, sent to server, waiting for reply/ies.
-record(pending_req,
        {
         command :: #command{},
         response_class :: ered_command:response_class() |
                           [ered_command:response_class()],
         reply_acc = []
        }).

%% Queue macro, can be used in guards.
-define(q_is_empty(Q), (element(1, Q) =:= 0)).

%% Commands like SUBSCRIBE and UNSUBSCRIBE don't return anything, so we use this
%% return value.
-define(pubsub_reply, undefined).

%%%===================================================================
%%% API
%%%===================================================================

%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-spec start_link(host(), inet:port_number(), [opt()]) ->
          {ok, pid()} | {error, term()}.
-spec start_link(host(), inet:port_number(), [opt()], pid()) ->
          {ok, pid()} | {error, term()}.
%%
%% Start the client process. Create a connection towards the provided
%% address. Typically called by a supervisor. Use connect/3 instead.
%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
start_link(Host, Port, Opts) ->
    start_link(Host, Port, Opts, self()).
start_link(Host, Port, Opts, User) ->
    gen_server:start_link(?MODULE, {Host, Port, Opts, User}, []).

%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-spec connect(host(), inet:port_number(), [opt()]) ->
          {ok, pid()} | {error, term()}.
%%
%% Create a standalone connection supervised by the ered application.
%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
connect(Host, Port, Opts) ->
    try ered_client_sup:start_client(ered_standalone_sup, Host, Port, Opts, self()) of
        {ok, ClientPid} ->
            {ok, ClientPid}
    catch exit:{noproc, _} ->
            {error, ered_not_started}
    end.

%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-spec close(pid()) -> ok.
%%
%% Stop the client process. Cancel all commands in queue. Take down
%% connection.
%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
close(ServerRef) ->
    gen_server:stop(ServerRef).

%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-spec deactivate(pid()) -> ok.
%%
%% Prepares the client to stop. Cancel all commands in queue and put
%% the client in a 'node_deactivated' state. The client is still
%% running though, and can be reactivated if the node comes back to the
%% cluster before the client is stopped.
%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
deactivate(ServerRef) ->
    gen_server:cast(ServerRef, deactivate).

%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-spec reactivate(pid()) -> ok.
%%
%% Reactivates a client that has previously been deactivated. This is
%% done when a node comes back to a cluster before the client for that
%% node has been stopped.
%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
reactivate(ServerRef) ->
    gen_server:cast(ServerRef, reactivate).

%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-spec command(pid(), ered_command:command()) -> reply().
-spec command(pid(), ered_command:command(), timeout()) -> reply().
%%
%% Send a command to the connected node. The argument can be a
%% single command as a list of binaries, a pipeline of command as a
%% list of commands or a formatted redis_command.
%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
command(ServerRef, Command) ->
    command(ServerRef, Command, infinity).

command(ServerRef, Command, Timeout) ->
    gen_server:call(ServerRef, {command, ered_command:convert_to(Command)}, Timeout).

%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-spec command_async(pid(), ered_command:command(), reply_fun()) -> ok.
%%
%% Send a command to the connected node in asynchronous
%% fashion. The provided callback function will be called with the
%% reply. Note that the callback function will executing in the redis
%% client process and should not hang or perform any lengthy task.
%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
command_async(ServerRef, Command, CallbackFun) ->
    gen_server:cast(ServerRef, #command{data = ered_command:convert_to(Command),
                                        replyto = CallbackFun}).

%% Converts a state record to a map, for easier testing.
%% Used in tests, after calling sys:get_state(EredClientPid).
state_to_map(#st{} = State) ->
    Fields = record_info(fields, st),
    [st | Values] = tuple_to_list(State),
    maps:from_list(lists:zip(Fields, Values)).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================
init({Host, Port, OptsList, User}) ->
    Opts = lists:foldl(
             fun({connection_opts, Val}, S)   -> handle_connection_opts(S, Val);
                ({max_waiting, Val}, S)       -> S#opts{max_waiting = Val};
                ({max_pending, Val}, S)       -> S#opts{max_pending = Val};
                ({queue_ok_level, Val}, S)    -> S#opts{queue_ok_level = Val};
                ({reconnect_wait, Val}, S)    -> S#opts{reconnect_wait = Val};
                ({info_pid, Val}, S)          -> S#opts{info_pid = Val};
                ({resp_version, Val}, S)      -> S#opts{resp_version = Val};
                ({node_down_timeout, Val}, S) -> S#opts{node_down_timeout = Val};
                ({use_cluster_id, Val}, S)    -> S#opts{use_cluster_id = Val};
                ({auth, Auth = {_, _}}, S)    -> S#opts{auth = Auth};
                ({select_db, Val}, S)         -> S#opts{select_db = Val};
                (Other, _)                    -> error({badarg, Other})
             end,
             #opts{host = Host, port = Port},
             OptsList),
    monitor(process, User),
    process_flag(trap_exit, true),
    State0 = #st{opts = Opts,
                 controlling_process = User,
                 parser_state = ered_parser:init(),
                 status = init},
    State1 = start_connect_loop(now, State0),
    {ok, start_node_down_timer(State1)}.

%% "Connection opts" is second layer of options.
%%
%% TODO: Remove this layering and put them directly as client options. It's an
%% API change so we'll do it in an appropriate version.
handle_connection_opts(OptsRecord, Opts) ->
    Valid = [batch_size, tcp_options, tls_options, push_cb, response_timeout,
             tcp_connect_timeout, tls_connect_timeout, connect_timeout],
    [error({badarg, BadOpt})
     || BadOpt <- proplists:get_keys(Opts) -- Valid],
    BatchSize = proplists:get_value(batch_size, Opts, 16),
    ResponseTimeout = proplists:get_value(response_timeout, Opts, 10000),
    PushCb = proplists:get_value(push_cb, Opts, fun(_) -> ok end),
    TcpOptions = proplists:get_value(tcp_options, Opts, []),
    TlsOptions = proplists:get_value(tls_options, Opts, []),
    TcpTimeout = proplists:get_value(tcp_connect_timeout, Opts, infinity),
    TlsTimeout = proplists:get_value(tls_connect_timeout, Opts, infinity),
    {Transport, Options, Timeout0} = case TlsOptions of
                                         [] ->
                                             {gen_tcp, TcpOptions, TcpTimeout};
                                         _ ->
                                             {ssl, TlsOptions, TlsTimeout}
                                     end,
    ConnectTimeout = proplists:get_value(connect_timeout, Opts, Timeout0),
    OptsRecord#opts{batch_size = BatchSize,
                    transport = Transport, transport_opts = Options,
                    connect_timeout = ConnectTimeout,
                    timeout = ResponseTimeout,
                    push_cb = PushCb}.

handle_call({command, Command}, From, State) ->
    Fun = fun(Reply) -> gen_server:reply(From, Reply) end,
    handle_cast(#command{data = Command, replyto = Fun}, State).


handle_cast(Command = #command{}, State) ->
    case State#st.status of
        Up when Up =:= up; Up =:= init ->
            State1 = State#st{waiting = q_in(Command, State#st.waiting)},
            State2 = process_commands(State1),
            {noreply, State2, response_timeout(State2)};
        NodeProblem when NodeProblem =:= node_down; NodeProblem =:= node_deactivated ->
            reply_command(Command, {error, NodeProblem}),
            {noreply, State}
    end;

handle_cast(deactivate, State) ->
    State1 = cancel_node_down_timer(State),
    State2 = report_connection_status(node_deactivated, State1),
    State3 = reply_all({error, node_deactivated}, State2),
    {noreply, process_commands(State3#st{status = node_deactivated})};

handle_cast(reactivate, #st{socket = none} = State) ->
    {noreply, start_node_down_timer(State)};

handle_cast(reactivate, State) ->
    {noreply, State#st{status = up}}.

handle_info({Type, Socket, Data}, #st{socket = Socket} = State)
  when Type =:= tcp; Type =:= ssl ->
    %% Receive data from current socket.
    State1 = handle_data(Data, State),
    State2 = process_commands(State1),
    {noreply, State2, response_timeout(State2)};

handle_info({Passive, Socket}, #st{socket = Socket} = State)
  when Passive =:= tcp_passive; Passive =:= ssl_passive ->
    %% Socket switched to passive mode due to {active, N}.
    %% TODO: Add config for N.
    N = 100,
    case setopts(State, [{active, N}]) of
        ok ->
            {noreply, State, response_timeout(State)};
        {error, Reason} ->
            Transport = State#st.opts#opts.transport,
            Transport:close(Socket),
            {noreply, connection_down({socket_closed, Reason}, State#st{socket = undefined})}
    end;

handle_info({Error, Socket, Reason}, #st{socket = Socket} = State)
  when Error =:= tcp_error; Error =:= ssl_error ->
    %% Socket errors. If the network or peer is down, the error is not
    %% always followed by a tcp_closed.
    %%
    %% TLS 1.3: Called after a connect when the client certificate has expired
    Transport = State#st.opts#opts.transport,
    Transport:close(Socket),
    {noreply, connection_down({socket_closed, Reason}, State#st{socket = none})};

handle_info({Closed, Socket}, #st{socket = Socket} = State)
  when Closed =:= tcp_closed; Closed =:= ssl_closed ->
    %% Socket got closed by the server.
    {noreply, connection_down({socket_closed, Closed}, State#st{socket = none})};

handle_info(ConnectError = {connect_error, _Reason}, State) ->
    %% Message from the connect loop process. It will retry.
    {noreply, connection_down(ConnectError, State)};

handle_info({connected, Socket}, State) ->
    %% Sent from connect loop process when just before it exits.
    State1 = abort_pending_commands(State),
    State2 = State1#st{socket = Socket,
                       connected_at = erlang:monotonic_time(millisecond),
                       status = init},
    State3 = init_connection(State2),
    {noreply, State3, response_timeout(State3)};

handle_info({init_command_reply, {ok, Replies}}, State) ->
    case [Reason || {error, Reason} <- Replies] of
        [] ->
            %% No errors
            ClusterId = case State#st.opts#opts.use_cluster_id of
                            true ->
                                hd(Replies);
                            false ->
                                undefined
                        end,
            State1 = cancel_node_down_timer(State),
            NodeStatus = case State1#st.status of
                             node_down -> up;
                             init      -> up;
                             OldStatus -> OldStatus
                         end,
            State2 = State1#st{status = NodeStatus, cluster_id = ClusterId},
            State3 = report_connection_status(connection_up, State2),
            {noreply, process_commands(State3), response_timeout(State3)};
        Errors ->
            {noreply, connection_down({init_error, Errors}, State)}
    end;
handle_info({init_command_reply, {error, Reason}}, State) ->
    {noreply, connection_down({init_error, Reason}, State)};

handle_info({timeout, TimerRef, node_down}, State) when TimerRef == State#st.node_down_timer ->
    %% Node down timeout
    State1 = report_connection_status({connection_down, node_down_timeout}, State),
    State2 = reply_all({error, node_down}, State1),
    {noreply, process_commands(State2#st{status = node_down})};

handle_info(timeout, #st{socket = Socket} = State) when Socket =/= none ->
    %% Request timeout
    Transport = State#st.opts#opts.transport,
    Transport:close(Socket),
    {noreply, connection_down({socket_closed, timeout}, State#st{socket = none})};

handle_info({'DOWN', _Mon, process, Pid, ExitReason}, State = #st{controlling_process = Pid}) ->
    {stop, ExitReason, State};

handle_info({'EXIT', Pid, normal}, #st{connection_loop_pid = Pid} = State) ->
    State1 = State#st{connection_loop_pid = none},
    State2 = case State1#st.socket of
                 none ->
                     %% Corner case. The new connection was lost before this
                     %% exit signal arrived. Start reconnect loop again.
                     start_connect_loop(now, State1);
                 _Socket ->
                     State1
             end,
    {noreply, State2, response_timeout(State2)};

handle_info({'EXIT', _From, Reason}, State) ->
    %% Supervisor exited.
    {stop, Reason, State};

handle_info(_Ignore, State) ->
    {noreply, State, response_timeout(State)}.

terminate(Reason, State) ->
    reply_all({error, {client_stopped, Reason}}, State),
    report_connection_status({connection_down, {client_stopped, Reason}}, State),
    ok.

code_change(_OldVsn, State = #st{opts = #opts{}}, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

setopts(#st{opts = #opts{transport = gen_tcp}, socket = Socket}, Opts) ->
    inet:setopts(Socket, Opts);
setopts(#st{opts = #opts{transport = ssl}, socket = Socket}, Opts) ->
    ssl:setopts(Socket, Opts).

%% Data received from the server
handle_data(Data, #st{parser_state = ParserState} = State) ->
    handle_parser_result(ered_parser:continue(Data, ParserState), State).

handle_parser_result({need_more, _BytesNeeded, ParserState}, State) ->
    State#st{parser_state = ParserState};
handle_parser_result({done, Value, ParserState}, State0) ->
    State1 = handle_result(Value, State0),
    handle_parser_result(ered_parser:next(ParserState), State1);
handle_parser_result({parse_error, Reason}, State) ->
    Transport = State#st.opts#opts.transport,
    Transport:close(State#st.socket),
    connection_down({socket_closed, {parse_error, Reason}}, State#st{socket = none}).

handle_result({push, Value = [Type|_]}, State) ->
    %% Pub/sub in RESP3 is a bit quirky. The push is supposed to be out of band
    %% data not connected to any request but for subscribe and unsubscribe
    %% requests, a successful command is signalled as one or more push messages.
    PushCB = State#st.opts#opts.push_cb,
    PushCB(Value),
    State1 = case is_subscribe_push(Type) of
                 true ->
                     handle_subscribe_push(Value, State);
                 false ->
                     State
             end,
    State1;
handle_result(Value, #st{pending = PendingQueue} = State)
  when not ?q_is_empty(PendingQueue) ->
    {PendingReq, PendingQueue1} = q_out(PendingQueue),
    #pending_req{command = Command,
                 response_class = RespClass,
                 reply_acc = Acc} = PendingReq,
    %% Check how many replies expected (list = pipeline)
    case RespClass of
        Single when not is_list(Single) ->
            reply_command(Command, {ok, Value}),
            State#st{pending = PendingQueue1};
        [_] ->
            %% Last one, send the reply
            reply_command(Command, {ok, lists:reverse([Value | Acc])}),
            State#st{pending = PendingQueue1};
        [_ | TailClasses] ->
            %% Need more replies. Save the reply and keep going.
            PendingReq1 = PendingReq#pending_req{response_class = TailClasses,
                                                 reply_acc = [Value | Acc]},
            PendingQueue2 = q_in_r(PendingReq1, PendingQueue1),
            State#st{pending = PendingQueue2}
    end;
handle_result(_Value, #st{pending = PendingQueue})
  when ?q_is_empty(PendingQueue) ->
    error(unexpected_reply).

is_subscribe_push(<<"subscribe">>) ->
    true;
is_subscribe_push(<<X, "subscribe">>) when X >= $a, X =< $z ->
    true;
is_subscribe_push(<<"unsubscribe">>) ->
    true;
is_subscribe_push(<<X, "unsubscribe">>) when X >= $a, X =< $z ->
    true;
is_subscribe_push(_) ->
    false.

handle_subscribe_push(PushMessage, #st{pending = PendingQueue} = State) ->
    case q_out(PendingQueue) of
        {PendingReq, PendingQueue1} ->
            State1 = State#st{pending = PendingQueue1},
            handle_subscribed_popped_pending(PushMessage, PendingReq, State1);
        empty ->
            %% No commands pending. It's may be a server initiated unsubscribe.
            State
    end.

handle_subscribed_popped_pending(Push,
                                 #pending_req{command = Command,
                                              response_class = ExpectClass,
                                              reply_acc = Acc} = Req,
                                 State) ->
    case {ExpectClass, hd(Push)} of
        {{Type, N}, Type}                       % simple command
          when N =:= 0;                         % unsubscribing from all channels
               N =:= 1 ->                       % or subscribed to all channels
            reply_command(Command, {ok, ?pubsub_reply}),
            State;
        {{Type, N}, Type}                       % simple command
          when N > 1 ->                         % not yet subscribed all channels
            Req1 = Req#pending_req{response_class = {Type, N - 1}},
            pending_in_r(Req1, State);
        {[{Type, N}], Type}                     % last command in pipeline
          when N =:= 0;                         % unsubscribing from all channels
               N =:= 1 ->                       % or subscribed to all channels
            reply_command(Command, {ok, lists:reverse([?pubsub_reply | Acc])}),
            State;
        {[{Type, N} | Classes], Type}           % pipeline, not the last command
          when N =:= 0;                         % unsubscribing from all channels
               N =:= 1 ->                       % or subscribed to all channels
            Req1 = Req#pending_req{response_class = Classes,
                                   reply_acc = [?pubsub_reply | Acc]},
            pending_in_r(Req1, State);
        {[{Type, N} | Classes], Type}           % pipeline
          when N > 1 ->                         % not yet subscribed all channels
            Req1 = Req#pending_req{response_class = [{Type, N - 1} | Classes]},
            pending_in_r(Req1, State);
        _Otherwise ->
            %% Not expecting this particular push message for Req. Put it back in queue.
            pending_in_r(Req, State)
    end.

%% Add in the front of the pending queue (like queue:in_r).
pending_in_r(ReplyInfo, #st{pending = Pending0} = State) ->
    Pending1 = q_in_r(ReplyInfo, Pending0),
    State#st{pending = Pending1}.

reply_all(Reply, State) ->
    [reply_command(Req#pending_req.command, Reply) || Req <- q_to_list(State#st.pending)],
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

%% Move pending commands back to the waiting queue. Discard partial replies.
%%
%% This is risky behavior. If some of the commands sent are not idempotent, we
%% can't just reconnect and send them again. We may just want to return an error
%% instead.
abort_pending_commands(State) ->
    PendingReqs = [Req#pending_req.command || Req <- q_to_list(State#st.pending)],
    State#st{waiting = q_join(q_from_list(PendingReqs), State#st.waiting),
             pending = q_new(),
             parser_state = ered_parser:init(),
             filling_batch = true}.

connection_down(Reason, State) ->
    State1 = abort_pending_commands(State),
    State2 = process_commands(State1),
    State3 = report_connection_status({connection_down, Reason}, State2),
    State4 = start_connect_loop(now, State3),
    start_node_down_timer(State4).

-spec process_commands(#st{}) -> #st{}.
process_commands(State) ->
    NumWaiting = q_len(State#st.waiting),
    NumPending = q_len(State#st.pending),
    BatchSize =  State#st.opts#opts.batch_size,
    LowWaterMark = max(State#st.opts#opts.max_pending - BatchSize, 1),

    if
        State#st.status =:= up, State#st.socket =/= none,
        NumWaiting > 0,  State#st.filling_batch ->
            %% TODO: Add request timeout timestamp to PendingReq.
            {CommandQueue, NewWaiting} = q_split(min(BatchSize, NumWaiting), State#st.waiting),
            {BatchedData, PendingRequests} =
                lists:foldr(fun(Command, {DataAcc, PendingAcc}) ->
                                    RespCommand = Command#command.data,
                                    ResponseClass = ered_command:get_response_class(RespCommand),

                                    NewBatchedData = ered_command:get_data(RespCommand),
                                    NewPendingRequest = #pending_req{command = Command,
                                                                     response_class = ResponseClass},
                                    {[NewBatchedData | DataAcc] , q_in_r(NewPendingRequest, PendingAcc)}
                            end,
                            {[], q_new()},
                            q_to_list(CommandQueue)),
            Transport = State#st.opts#opts.transport,
            case Transport:send(State#st.socket, BatchedData) of
                ok ->
                    NewPending = q_join(State#st.pending, PendingRequests),
                    NewState = State#st{waiting = NewWaiting,
                                        pending = NewPending,
                                        filling_batch = q_len(NewPending) < State#st.opts#opts.max_pending},
                    process_commands(NewState);
                {error, _Reason} ->
                    %% Send FIN and handle replies in fligh before reconnecting.
                    Transport:shutdown(State#st.socket, read_write),
                    start_connect_loop(now, State#st{status = init})
            end;

        not State#st.filling_batch, NumPending < LowWaterMark ->
            process_commands(State#st{filling_batch = true});

        NumWaiting > State#st.opts#opts.max_waiting, State#st.queue_full_event_sent ->
            drop_commands(State);

        NumWaiting > State#st.opts#opts.max_waiting ->
            drop_commands(
              report_connection_status(queue_full, State#st{queue_full_event_sent = true}));

        NumWaiting < State#st.opts#opts.queue_ok_level, State#st.queue_full_event_sent ->
            report_connection_status(queue_ok, State#st{queue_full_event_sent = false});

        true ->
            State
    end.

start_connect_loop(_When, State) when is_pid(State#st.connection_loop_pid) ->
    State;
start_connect_loop(When0, State) ->
    Self = self(),
    Now = erlang:monotonic_time(millisecond),
    ConnectedAt = State#st.connected_at,
    %% Don't reconnect immediately if the last connect was too recently.
    When = if
               is_integer(ConnectedAt),
               Now - ConnectedAt < State#st.opts#opts.reconnect_wait ->
                   wait;
               true ->
                   When0
           end,
    ConnectPid = spawn_link(fun () -> connect_loop(When, Self, State#st.opts) end),
    State#st{connection_loop_pid = ConnectPid}.

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

q_in_r(Item, {Size, Q}) ->
    {Size + 1, queue:in_r(Item, Q)}.

q_join({Size1, Q1}, {Size2, Q2}) ->
    {Size1 + Size2, queue:join(Q1, Q2)}.

q_out({Size, Q}) ->
    case queue:out(Q) of
        {empty, _Q} -> empty;
        {{value, Val}, NewQ} -> {Val, {Size-1, NewQ}}
    end.

q_split(N, {Size, Q}) when N =< Size ->
    {A, B} = queue:split(N, Q),
    {{N, A}, {Size - N, B}}.

q_to_list({_Size, Q}) ->
    queue:to_list(Q).

q_from_list(List) ->
    {length(List), queue:from_list(List)}.

q_len({Size, _Q}) ->
    Size.

response_timeout(State) when not ?q_is_empty(State#st.pending) ->
    %% FIXME: Store req timeout in each pending item
    State#st.opts#opts.timeout;
response_timeout(_State) ->
    infinity.

reply_command(#command{replyto = Fun} = _Command, Reply) ->
    Fun(Reply).

-spec report_connection_status(status(), #st{}) -> #st{}.
report_connection_status(Status, State = #st{last_status = Status}) ->
    State;
report_connection_status({connection_down, {init_error, node_down}},
                         #st{last_status = {connection_down, _}} = State) ->
    %% Silence additional init error cased by connection down. The lost
    %% connection was already reported in another status message.
    State;
report_connection_status({connection_down, {init_error, InitReason}},
                         #st{last_status = node_deactivated} = State)
  when InitReason =:= node_deactivated; InitReason =:= node_down  ->
    %% Silence additional init error when node is deactivated.
    State;
report_connection_status(Status, State) ->
    send_info(Status, State),
    case Status of
        %% Skip saving the last_status in this to avoid an extra connect_error event.
        %% The usual case is that there is a connect_error and then node_down and then
        %% more connect_errors..
        {connection_down, node_down_timeout} ->
            State;
        _ ->
            State#st{last_status = Status}
    end.


-spec send_info(status(), #st{}) -> ok.
send_info(Status, #st{opts = #opts{info_pid = Pid,
                                   host = Host,
                                   port = Port},
                      cluster_id = ClusterId}) when is_pid(Pid) ->
    {MsgType, Reason} =
        case Status of
            connection_up                        -> {connected, none};
            {connection_down, R} when is_atom(R) -> {R, none};
            {connection_down, R}                 -> R;
            node_deactivated                     -> {node_deactivated, none};
            queue_full                           -> {queue_full, none};
            queue_ok                             -> {queue_ok, none}
        end,
    Msg0 = #{msg_type  => MsgType,
             reason    => Reason,
             addr      => {Host, Port},
             client_id => self()},
    Msg = case ClusterId of
              undefined ->
                  Msg0;
              Id when is_binary(Id) ->
                  Msg0#{cluster_id => ClusterId}
          end,
    Pid ! Msg,
    ok;
send_info(_Msg, _State) ->
    ok.

%% Connect-wait-retry loop, to run in a separate spawned process. When
%% connected, transfers the socket to the OwnerPid, sends a message `{connected,
%% Socket}` and exits. On connect error, a message `{connect_error, Reason}` is
%% sent and connecting is retried periodically.
connect_loop(now, OwnerPid,
             #opts{host = Host, port = Port, transport = Transport,
                   transport_opts = TransportOpts0,
                   connect_timeout = Timeout} = Opts) ->
    TransportOpts = [{active, 100}, binary] ++ TransportOpts0,
    case Transport:connect(Host, Port, TransportOpts, Timeout) of
        {ok, Socket} ->
            case Transport:controlling_process(Socket, OwnerPid) of
                ok ->
                    OwnerPid ! {connected, Socket};
                {error, Reason} ->
                    OwnerPid ! {connect_error, Reason},
                    Transport:close(Socket),
                    connect_loop(wait, OwnerPid, Opts)
            end;
        {error, Reason} ->
            OwnerPid ! {connect_error, Reason},
            connect_loop(wait, OwnerPid, Opts)
    end;
connect_loop(wait, OwnerPid, Opts) ->
    timer:sleep(Opts#opts.reconnect_wait),
    connect_loop(now, OwnerPid, Opts).

init_connection(State) ->
    #st{opts = #opts{transport = Transport} = Opts,
        socket = Socket} = State,
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
    Cmd3 = [[<<"SELECT">>, integer_to_binary(Opts#opts.select_db)] ||
               Opts#opts.select_db > 0],
    case Cmd1 ++ Cmd2 ++ Cmd3 of
        [] ->
            self() ! {init_command_reply, {ok, []}},
            State;
        Pipeline ->
            %% Add to pending queue and send like any other commands.
            ReplyFun = fun (Reply) ->
                               self() ! {init_command_reply, Reply}
                       end,
            RespCommand = ered_command:convert_to(Pipeline),
            Data = ered_command:get_data(RespCommand),
            Command = #command{data = RespCommand, replyto = ReplyFun},
            Class = ered_command:get_response_class(RespCommand),
            PendingReq = #pending_req{command = Command, response_class = Class},
            Transport = State#st.opts#opts.transport,
            case Transport:send(State#st.socket, Data) of
                ok ->
                    State1 = State#st{pending = q_in(PendingReq, State#st.pending),
                                      status = init},
                    %% Send commands immediately or wait for init reply first?
                    %% process_commands(State1);
                    State1;
                {error, _Reason} ->
                    %% Send FIN and handle replies in flight before reconnecting.
                    Transport:shutdown(Socket, read_write),
                    start_connect_loop(wait, State)
            end
    end.
