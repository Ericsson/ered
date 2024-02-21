-module(ered).

%% External API for using connecting and sending commands to Redis cluster.
%%
%% This module is responsible for doing the command routing to the correct
%% redis node and handle command redirection replies.

-behaviour(gen_server).

%% API
-export([start_link/2,
         stop/1,
         command/3, command/4,
         command_async/4,
         command_all/2, command_all/3,
         command_client/2, command_client/3,
         command_client_async/3,
         get_clients/1,
         get_addr_to_client_map/1,
         update_slots/1, update_slots/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3, format_status/2]).

-export_type([opt/0,
              command/0,
              reply/0,
              reply_fun/0,
              client_ref/0,
              server_ref/0]).

%%%===================================================================
%%% Definitions
%%%===================================================================

-record(st, {cluster_pid :: pid(),
             slots :: binary(),       % The byte att offset N is an index into
                                                % the clients tuple for slot N.
             clients = {} :: tuple(), % ... of pid() (or addr() as placeholder)
             slot_map_version = 0 :: non_neg_integer(),
             addr_map = #{} :: #{addr() => pid() | Placeholder :: addr()},
             pending = #{} :: #{gen_server:from() => pid()},
             try_again_delay :: non_neg_integer(),
             redirect_attempts :: non_neg_integer()
            }).

-type opt() ::
        %% If there is a TRYAGAIN response from Redis then wait
        %% this many milliseconds before re-sending the command
        {try_again_delay, non_neg_integer()} |
        %% Only do these many retries or re-sends before giving
        %% up and returning the result. This affects ASK, MOVED
        %% and TRYAGAIN responses
        {redirect_attempts, non_neg_integer()} |
        ered_cluster:opt().

-type addr()       :: ered_cluster:addr().
-type server_ref() :: pid().
-type command()    :: ered_command:command().
-type reply()      :: ered_client:reply().
-type reply_fun()  :: ered_client:reply_fun().
-type key()        :: binary().
-type client_ref() :: ered_client:server_ref().

%%%===================================================================
%%% API
%%%===================================================================

%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-spec start_link([addr()], [opt()]) -> {ok, server_ref()} | {error, term()}.
%%
%% Start the main process. This will also start the cluster handling
%% process which will set up clients to the provided addresses and
%% fetch the cluster slot map. Once there is a complete slot map and
%% all Redis node clients are connected this process is ready to
%% server requests.
%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
start_link(Addrs, Opts) ->
    gen_server:start_link(?MODULE, [Addrs, Opts], []).

%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-spec stop(server_ref()) -> ok.
%%
%% Stop the main process. This will also stop the cluster handling
%% process and in turn disconnect and stop all clients.
%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
stop(ServerRef) ->
    gen_server:stop(ServerRef).

%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-spec command(server_ref(), command(), key()) -> reply().
-spec command(server_ref(), command(), key(), timeout()) -> reply().
%%
%% Send a command to the Redis cluster. The command will be routed to
%% the correct Redis node client based on the provided key.
%% If the command is a single command then it is represented as a
%% list of binaries where the first binary is the Redis command
%% to execute and the rest of the binaries are the arguments.
%% If the command is a pipeline, e.g. multiple commands to executed
%% then they need to all map to the same slot for things to
%% work as expected.
%% Command/3 is the same as setting the timeout to infinity.
%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
command(ServerRef, Command, Key) ->
    command(ServerRef, Command, Key, infinity).

command(ServerRef, Command, Key, Timeout) ->
    C = ered_command:convert_to(Command),
    gen_server:call(ServerRef, {command, C, Key}, Timeout).

%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-spec command_async(server_ref(), command(), key(), fun((reply()) -> any())) -> ok.
%%
%% Like command/3,4 but asynchronous. Instead of returning the reply, the reply
%% function is applied to the reply when it is available. The reply function
%% runs in an unspecified process.
%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
command_async(ServerRef, Command, Key, ReplyFun) when is_function(ReplyFun, 1) ->
    C = ered_command:convert_to(Command),
    gen_server:cast(ServerRef, {command_async, C, Key, ReplyFun}).

%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-spec command_all(server_ref(), command()) -> [reply()].
-spec command_all(server_ref(), command(), timeout()) -> [reply()].
%%
%% Send the same command to all connected master Redis nodes.
%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
command_all(ServerRef, Command) ->
    command_all(ServerRef, Command, infinity).

command_all(ServerRef, Command, Timeout) ->
    %% Send command in sequence to all instances.
    %% This could be done in parallel but but keeping it easy and
    %% aligned with eredis_cluster for now
    Cmd = ered_command:convert_to(Command),
    [ered_client:command(ClientRef, Cmd, Timeout) || ClientRef <- get_clients(ServerRef)].

%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-spec command_client(client_ref(), command()) -> reply().
-spec command_client(client_ref(), command(), timeout()) -> reply().
%%
%% Send the command to a specific Redis client without any client routing.
%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
command_client(ClientRef, Command) ->
    command_client(ClientRef, Command, infinity).

command_client(ClientRef, Command, Timeout) ->
    ered_client:command(ClientRef, Command, Timeout).

%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-spec command_client_async(client_ref(), command(), reply_fun()) -> ok.
%%
%% Send command to a specific Redis client in asynchronous fashion. The
%% provided callback function will be called with the reply. Note that
%% the callback function will executing in the redis client process and
%% should not hang or perform any lengthy task.
%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
command_client_async(ClientRef, Command, CallbackFun) ->
    ered_client:command_async(ClientRef, Command, CallbackFun).

%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-spec get_clients(server_ref()) -> [client_ref()].
%%
%% Get all Redis master node clients
%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
get_clients(ServerRef) ->
    gen_server:call(ServerRef, get_clients).

%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-spec get_addr_to_client_map(server_ref()) -> #{addr() => client_ref()}.
%%
%% Get the address to client mapping. This includes all clients.
%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
get_addr_to_client_map(ServerRef) ->
    gen_server:call(ServerRef, get_addr_to_client_map).

%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-spec update_slots(server_ref()) -> ok.
%%
%% Trigger a slot-to-node mapping update using any connected client.
%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
update_slots(ServerRef) ->
    gen_server:cast(ServerRef, {update_slots, none}).

-spec update_slots(server_ref(), client_ref()) -> ok.
%%
%% Trigger a slot-to-node mapping update using the specified client,
%% if it's already connected. Otherwise another connected client is
%% used.
%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
update_slots(ServerRef, ClientRef) ->
    gen_server:cast(ServerRef, {update_slots, ClientRef}).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([Addrs, Opts1]) ->
    %% Register callback to get slot map updates
    InfoPids = [self() | proplists:get_value(info_pid, Opts1, [])],
    Opts2 = [{info_pid, InfoPids} | proplists:delete(info_pid, Opts1)],

    {TryAgainDelay, Opts3} = take_prop(try_again_delay, Opts2, 200),
    {RedirectAttempts, Opts4} = take_prop(redirect_attempts, Opts3, 10),

    {ok, ClusterPid} = ered_cluster:start_link(Addrs, Opts4),
    EmptySlots = create_lookup_table(0, [], <<>>),
    {ok, #st{cluster_pid = ClusterPid,
             slots = EmptySlots,
             try_again_delay = TryAgainDelay,
             redirect_attempts = RedirectAttempts}}.

handle_call({command, Command, Key}, From, State) ->
    Slot = ered_lib:hash(Key),
    State1 = send_command_to_slot(Command, Slot, From, State, State#st.redirect_attempts),
    {noreply, State1};

handle_call(get_clients, _From, State) ->
    {reply, tuple_to_list(State#st.clients), State};

handle_call(get_addr_to_client_map, _From, State) ->
    {reply, State#st.addr_map, State}.

handle_cast({command_async, Command, Key, ReplyFun}, State) ->
    Slot = ered_lib:hash(Key),
    State1 = send_command_to_slot(Command, Slot, ReplyFun, State, State#st.redirect_attempts),
    {noreply, State1};

handle_cast({replied, To}, State) ->
    {noreply, State#st{pending = maps:remove(To, State#st.pending)}};

handle_cast({update_slots, ClientRef}, State) ->
    ered_cluster:update_slots(State#st.cluster_pid, State#st.slot_map_version, ClientRef),
    {noreply, State};

handle_cast({forward_command, Command, Slot, From, Addr, AttemptsLeft}, State) ->
    {Client, State1} = connect_addr(Addr, State),
    Fun = create_reply_fun(Command, Slot, Client, From, State, AttemptsLeft),
    ered_client:command_async(Client, Command, Fun),
    {noreply, State1};

handle_cast({forward_command_asking, Command, Slot, From, Addr, AttemptsLeft, OldReply}, State) ->
    {Client, State1} = connect_addr(Addr, State),
    Command1 = ered_command:add_asking(OldReply, Command),
    HandleReplyFun = create_reply_fun(Command, Slot, Client, From, State, AttemptsLeft),
    Fun = fun(Reply) -> HandleReplyFun(ered_command:fix_ask_reply(OldReply, Reply)) end,
    ered_client:command_async(Client, Command1, Fun),
    {noreply, State1}.

handle_info({command_try_again, Command, Slot, From, AttemptsLeft}, State) ->
    State1 = send_command_to_slot(Command, Slot, From, State, AttemptsLeft),
    {noreply, State1};

handle_info(#{msg_type := slot_map_updated}, State) ->
    {MapVersion, ClusterMap, AddrToPid} = ered_cluster:get_slot_map_info(State#st.cluster_pid),
    %% The idea is to store the client pids in a tuple and then
    %% have a binary where each byte corresponds to a slot and the
    %% value maps to a index in the tuple.

    MasterAddrToPid = maps:with(ered_lib:slotmap_master_nodes(ClusterMap), AddrToPid),
    %% Create a list of indices, one for each client pid
    Ixs = lists:seq(1, maps:size(MasterAddrToPid)),
    %% Combine the indices with the Addresses to create a lookup from Addr -> Ix
    AddrToIx = maps:from_list(lists:zip(maps:keys(MasterAddrToPid), Ixs)),

    Slots = create_lookup_table(ClusterMap, AddrToIx),
    Clients = create_client_pid_tuple(MasterAddrToPid, AddrToIx),
    %% Monitor the client processes
    maps:foreach(fun (Addr, Pid) when map_get(Addr, State#st.addr_map) =:= Pid ->
                         ok; % Process already known
                     (Addr, Pid) ->
                         _ = monitor(process, Pid, [{tag, {'DOWN', Addr}}])
                 end,
                 AddrToPid),
    {noreply, State#st{slots = Slots,
                       clients = Clients,
                       slot_map_version = MapVersion,
                       addr_map = AddrToPid}};

handle_info(#{msg_type := connected, addr := Addr, client_id := Pid},
            State = #st{addr_map = AddrMap, clients = Clients})
  when is_map(State#st.addr_map) ->
    case maps:find(Addr, AddrMap) of
        {ok, Pid} ->
            %% We already have this pid.
            {noreply, State};
        {ok, OldPid} ->
            %% The pid has changed for this client. It was probably restarted.
            _Mon = monitor(process, Pid, [{tag, {'DOWN', Addr}}]),
            %% Replace the pid in our lookup tables.
            ClientList = [case P of
                              OldPid -> Pid;
                              Other -> Other
                          end || P <- tuple_to_list(Clients)],
            {noreply, State#st{addr_map = AddrMap#{Addr := Pid},
                               clients = list_to_tuple(ClientList)}};
        error ->
            _Mon = monitor(process, Pid, [{tag, {'DOWN', Addr}}]),
            {noreply, State#st{addr_map = AddrMap#{Addr => Pid}}}
    end;

handle_info({{'DOWN', Addr}, _Mon, process, Pid, ExitReason}, State)
  when map_get(Addr, State#st.addr_map) =:= Pid ->
    %% Client process is down. Abort all requests to this client.
    Pending = maps:fold(fun (From, To, Acc) when To =:= Pid ->
                                gen_server:reply(From, {error, ExitReason}),
                                maps:remove(From, Acc);
                            (_From, _To, Acc) ->
                                Acc
                        end,
                        State#st.pending,
                        State#st.pending),
    %% Put a placeholder instead of a pid in the lookup structures.
    Placeholder = Addr,
    ClientList = [case P of
                      Pid -> Placeholder;
                      Other -> Other
                  end || P <- tuple_to_list(State#st.clients)],
    {noreply, State#st{addr_map = (State#st.addr_map)#{Addr := Placeholder},
                       clients = list_to_tuple(ClientList),
                       pending = Pending}};

handle_info(_Ignore, State) ->
    {noreply, State}.

terminate(_Reason, State) ->
    ered_cluster:stop(State#st.cluster_pid),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

format_status(_Opt, Status) ->
    Status.

%%%===================================================================
%%% Internal functions
%%%===================================================================
send_command_to_slot(Command, Slot, From, State, AttemptsLeft) ->
    case binary:at(State#st.slots, Slot) of
        0 ->
            reply(From, {error, unmapped_slot}, none),
            State;
        Ix ->
            case element(Ix, State#st.clients) of
                Client when is_pid(Client) ->
                    Fun = create_reply_fun(Command, Slot, Client, From, State, AttemptsLeft),
                    ered_client:command_async(Client, Command, Fun),
                    put_pending(From, Client, State);
                _Placeholder ->
                    reply(From, {error, client_down}, none),
                    State
            end
    end.

put_pending(From = {_, _}, Client, State) ->
    %% Gen_server call. Store so we can reply if Client crashes.
    State#st{pending = maps:put(From, Client, State#st.pending)};
put_pending(ReplyFun, _Client, State) when is_function(ReplyFun) ->
    %% Cast with reply fun. We don't keep track of those.
    State.

create_reply_fun(_Command, _Slot, _Client, From, _State, 0) ->
    Pid = self(),
    fun(Reply) -> reply(From, Reply, Pid) end;

create_reply_fun(Command, Slot, Client, From, State, AttemptsLeft) ->
    Pid = self(),
    %% Avoid binding the #st record inside the fun since the fun will be
    %% copied to another process
    ClusterPid = State#st.cluster_pid,
    SlotMapVersion = State#st.slot_map_version,
    TryAgainDelay = State#st.try_again_delay,
    fun(Reply) ->
            case ered_command:check_result(Reply) of
                normal ->
                    reply(From, Reply, Pid);
                {moved, Addr} ->
                    ered_cluster:update_slots(ClusterPid, SlotMapVersion, Client),
                    gen_server:cast(Pid, {forward_command, Command, Slot, From, Addr, AttemptsLeft-1});
                {ask, Addr} ->
                    gen_server:cast(Pid, {forward_command_asking, Command, Slot, From, Addr, AttemptsLeft-1, Reply});
                try_again ->
                    erlang:send_after(TryAgainDelay, Pid, {command_try_again, Command, Slot, From, AttemptsLeft-1});
                cluster_down ->
                    ered_cluster:update_slots(ClusterPid, SlotMapVersion, Client),
                    reply(From, Reply, Pid)
            end
    end.

%% Handle a reply, either by sending it back to a gen server caller or by
%% applying a reply function.
reply(To = {_, _}, Reply, EredPid) when is_pid(EredPid) ->
    gen_server:reply(To, Reply),
    gen_server:cast(EredPid, {replied, To});
reply(To = {_, _}, Reply, none) ->
    gen_server:reply(To, Reply);
reply(ReplyFun, Reply, _EredPid) when is_function(ReplyFun, 1) ->
    ReplyFun(Reply).

create_client_pid_tuple(AddrToPid, AddrToIx) ->
    %% Create a list with tuples where the first element is the index and the second is the pid
    IxPid = [{maps:get(Addr, AddrToIx), Pid} || {Addr, Pid} <- maps:to_list(AddrToPid)],
    %% Sort the list and remove the index to get the pids in the right order
    Pids = [Pid || {_Ix, Pid} <- lists:sort(IxPid)],
    list_to_tuple(Pids).

create_lookup_table(ClusterMap, AddrToIx) ->
    %% Replace the Addr in the slot map with the index using the lookup
    Slots = [{Start, End, maps:get(Addr,AddrToIx)}
             || {Start, End, Addr} <- ered_lib:slotmap_master_slots(ClusterMap)],
    create_lookup_table(0, Slots, <<>>).

create_lookup_table(16384, _, Acc) ->
    Acc;
create_lookup_table(N, [], Acc) ->
    %% no more slots, rest are set to unmapped
    create_lookup_table(N+1, [], <<Acc/binary,0>>);
create_lookup_table(N, L = [{Start, End, Val} | Rest], Acc) ->
    if
        N < Start -> % unmapped, use 0
            create_lookup_table(N+1, L, <<Acc/binary,0>>);
        N =< End -> % in range
            create_lookup_table(N+1, L, <<Acc/binary,Val>>);
        true ->
            create_lookup_table(N, Rest, Acc)
    end.

connect_addr(Addr, State) ->
    case maps:get(Addr, State#st.addr_map, not_found) of
        not_found ->
            Client = ered_cluster:connect_node(State#st.cluster_pid, Addr),
            _Mon = monitor(process, Client, [{tag, {'DOWN', Addr}}]),
            {Client, State#st{addr_map = maps:put(Addr, Client, State#st.addr_map)}};
        Client ->
            {Client, State}
    end.

take_prop(Key, List, Default) ->
    Val = proplists:get_value(Key, List, Default),
    NewList = proplists:delete(Key, List),
    {Val, NewList}.
