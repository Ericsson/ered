
-module(redis).

-behaviour(gen_server).

%% API
-export([start_link/2,
         stop/1,
         command/3, command/4,
         command_all/2, command_all/3,
         command_client/2, command_client/3,
         command_client_async/3,
         get_clients/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3, format_status/2]).


-record(st, {cluster_pid :: pid(),
             slots :: binary(),
             clients = {} :: tuple(), % of pid()
             slot_map_version = 0 :: non_neg_integer(),
             addr_map = {},

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
        redis_cluster2:opt().

-type addr() :: redis_cluster2:addr().
-type server_ref() :: pid().
-type command() :: redis_command:raw_command().
-type command_pipeline() :: redis_command:raw_command_pipeline().
-type reply() :: redis_client:command_reply().
-type reply_pipeline() :: redis_client:command_reply_pipeline().
-type key() :: binary().
-type client_ref() :: redis_client:client_ref().

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
-spec command(server_ref(), command(), key()) -> reply();
             (server_ref(), command_pipeline(), key()) -> reply_pipeline().

-spec command(server_ref(), command(), key(), timeout()) -> reply();
             (server_ref(), command_pipeline(), key(), timeout()) -> reply_pipeline().
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
    C = redis_command:convert_to(Command),
    gen_server:call(ServerRef, {command, C, Key}, Timeout).

%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-spec command_all(server_ref(), command()) -> [reply()];
                 (server_ref(), command_pipeline()) -> [reply_pipeline()].

-spec command_all(server_ref(), command(), timeout()) -> [reply()];
                 (server_ref(), command_pipeline(), timeout()) -> [reply_pipeline()].
%%
%% Send the same command to all connected master Redis nodes.
%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
command_all(ServerRef, Command) ->
    command_all(ServerRef, Command, infinity).

command_all(ServerRef, Command, Timeout) ->
    %% Send command in sequence to all instances.
    %% This could be done in parallel but but keeping it easy and
    %% aligned with eredis_cluster for now
    Cmd = redis_command:convert_to(Command),
    [redis_client:command(ClientRef, Cmd, Timeout) || ClientRef <- get_clients(ServerRef)].

%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-spec command_client(client_ref(), command()) -> [reply()];
                    (client_ref(), command_pipeline()) -> [reply_pipeline()].

-spec command_client(client_ref(), command(), timeout()) -> [reply()];
                    (client_ref(), command_pipeline(), timeout()) -> [reply_pipeline()].
%%
%% Send the command to a specific Redis client without any client routing.
%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
command_client(ClientRef, Command) ->
    command_client(ClientRef, Command, infinity).

command_client(ClientRef, Command, Timeout) ->
    redis_client:command(ClientRef, Command, Timeout).

%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-spec command_client_async(client_ref(), command(), fun()) -> ok;
                          (client_ref(), command_pipeline(), fun()) -> ok.

%%
%% Send command to a specific Redis client in asynchronous fashion. The
%% provided callback function will be called with the reply. Note that
%% the callback function will executing in the redis client process and
%% should not hang or perform any lengthy task.
%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
command_client_async(ClientRef, Command, CallbackFun) ->
    redis_client:command_async(ClientRef, Command, CallbackFun).

%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-spec get_clients(server_ref()) -> [client_ref()].
%%
%% Get all Redis master node clients
%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
get_clients(ServerRef) ->
    gen_server:call(ServerRef, get_clients).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([Addrs, Opts1]) ->
    %% Register callback to get slot map updates
    InfoPids = [self() | proplists:get_value(info_pid, Opts1, [])],
    Opts2 = [{info_pid, InfoPids} | proplists:delete(info_pid, Opts1)],

    {TryAgainDelay, Opts3} = take_prop(try_again_delay, Opts2, 200),
    {RedirectAttempts, Opts4} = take_prop(redirect_attempts, Opts3, 10),

    {ok, ClusterPid} = redis_cluster2:start_link(Addrs, Opts4),
    EmptySlots = create_lookup_table(0, [], <<>>),
    {ok, #st{cluster_pid = ClusterPid,
             slots = EmptySlots,
             try_again_delay = TryAgainDelay,
             redirect_attempts = RedirectAttempts}}.


handle_call({command, Command, Key}, From, State) ->
    Slot = redis_lib:hash(Key),
    send_command_to_slot(Command, Slot, From, State, State#st.redirect_attempts),
    {noreply, State};

handle_call(get_clients, _From, State) ->
    {reply, tuple_to_list(State#st.clients), State}.


handle_cast({forward_command, Command, Slot, From, Addr, AttemptsLeft}, State) ->
    {Client, State1} = connect_addr(Addr, State),
    Fun = create_reply_fun(Command, Slot, Client, From, State, AttemptsLeft),
    redis_client:command_async(Client, Command, Fun),
    {noreply, State1};

handle_cast({forward_command_asking, Command, Slot, From, Addr, AttemptsLeft, OldReply}, State) ->
    {Client, State1} = connect_addr(Addr, State),
    Command1 = redis_command:add_asking(OldReply, Command),
    HandleReplyFun = create_reply_fun(Command, Slot, Client, From, State, AttemptsLeft),
    Fun = fun(Reply) -> HandleReplyFun(redis_command:fix_ask_reply(OldReply, Reply)) end,
    redis_client:command_async(Client, Command1, Fun),
    {noreply, State1}.

handle_info({command_try_again, Command, Slot, From, AttemptsLeft}, State) ->
    send_command_to_slot(Command, Slot, From, State, AttemptsLeft),
    {noreply, State};



handle_info(#{msg_type := slot_map_updated}, State) ->
    {MapVersion, ClusterMap, AddrToPid} = redis_cluster2:get_slot_map_info(State#st.cluster_pid),
    %% The idea is to store the client pids in a tuple and then
    %% have a binary where each byte corresponds to a slot and the
    %% value maps to a index in the tuple.

    MasterAddrToPid = maps:with(redis_lib:slotmap_master_nodes(ClusterMap), AddrToPid),
    %% Create a list of indices, one for each client pid
    Ixs = lists:seq(1, maps:size(MasterAddrToPid)),
    %% Combine the indices with the Addresses to create a lookup from Addr -> Ix
    AddrToIx = maps:from_list(lists:zip(maps:keys(MasterAddrToPid), Ixs)),

    Slots = create_lookup_table(ClusterMap, AddrToIx),
    Clients = create_client_pid_tuple(MasterAddrToPid, AddrToIx),
    {noreply, State#st{slots = Slots,
                       clients = Clients,
                       slot_map_version = MapVersion,
                       addr_map = AddrToPid}};


handle_info(_Ignore, State) ->
    %% Could use a proxy process to receive the slot map udate to avoid this catch all handle_info
    {noreply, State}.


terminate(_Reason, State) ->
    redis_cluster2:stop(State#st.cluster_pid),
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
            gen_server:reply(From, {error, unmapped_slot});
        Ix ->
            Client = element(Ix, State#st.clients),
            Fun = create_reply_fun(Command, Slot, Client, From, State, AttemptsLeft),
            redis_client:command_async(Client, Command, Fun)
    end,
    ok.


create_reply_fun(_Command, _Slot, _Client, From, _State, 0) ->
    fun(Reply) -> gen_server:reply(From, Reply) end;

create_reply_fun(Command, Slot, Client, From, State, AttemptsLeft) ->
    Pid = self(),
    fun(Reply) ->
            case redis_command:check_result(Reply) of
                normal ->
                    gen_server:reply(From, Reply);
                {moved, Addr} ->
                    redis_cluster2:update_slots(State#st.cluster_pid, State#st.slot_map_version, Client),
                    gen_server:cast(Pid, {forward_command, Command, Slot, From, Addr, AttemptsLeft-1});
                {ask, Addr} ->
                    gen_server:cast(Pid, {forward_command_asking, Command, Slot, From, Addr, AttemptsLeft-1, Reply});
                try_again ->
                    erlang:send_after(State#st.try_again_delay, Pid, {command_try_again, Command, Slot, From, AttemptsLeft-1})
            end
    end.

create_client_pid_tuple(AddrToPid, AddrToIx) ->
    %% Create a list with tuples where the first element is the index and the second is the pid
    IxPid = [{maps:get(Addr, AddrToIx), Pid} || {Addr, Pid} <- maps:to_list(AddrToPid)],
    %% Sort the list and remove the index to get the pids in the right order
    Pids = [Pid || {_Ix, Pid} <- lists:sort(IxPid)],
    list_to_tuple(Pids).

create_lookup_table(ClusterMap, AddrToIx) ->
    %% Replace the Addr in the slot map with the index using the lookup
    Slots = [{Start, End, maps:get(Addr,AddrToIx)}
             || {Start, End, Addr} <- redis_lib:slotmap_master_slots(ClusterMap)],
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
            Client = redis_cluster2:connect_node(State#st.cluster_pid, Addr),
            {Client, State#st{addr_map = maps:put(Addr, Client, State#st.addr_map)}};
        Client ->
            {Client, State}
    end.


take_prop(Key, List, Default) ->
    Val = proplists:get_value(Key, List, Default),
    NewList = proplists:delete(Key, List),
    {Val, NewList}.
