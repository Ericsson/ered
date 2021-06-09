
-module(redis).

-behaviour(gen_server).

%% API
-export([start_link/3, command/3, command/4]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3, format_status/2]).


-record(st, {cluster_pid :: pid(),
             slots :: binary(),
             connections = {} :: tuple() % of pid()
            }).

%%%===================================================================
%%% API
%%%===================================================================
start_link(Host, Port, Opts) ->
    gen_server:start_link(?MODULE, [Host, Port, Opts], []).

command(ServerRef, Command, Key) ->
    command(ServerRef, Command, Key, infinity).

command(ServerRef, Command, Key, Timeout) ->
    C = redis_lib:format_request(Command),
    gen_server:call(ServerRef, {command, C, Key}, Timeout).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([Host, Port, Opts]) ->
    %% Register callback to get slot map updates
    Pid = self(),
    Opts2 = case lists:keytake(info_cb, 1, Opts) of
                false ->
                    [{info_cb, fun(Msg) -> info_cb(Pid, Msg) end} | Opts];
                {value, {info_cb, Fun}, Opts1} ->
                    [{info_cb, fun(Msg) -> info_cb(Pid, Msg), Fun(Msg) end} | Opts1]
            end,
    {ok, ClusterPid} = redis_cluster2:start_link(Host, Port, Opts2),
    EmptySlots = create_lookup_table(0, [], <<>>),
    {ok, #st{cluster_pid = ClusterPid, slots = EmptySlots}}.


handle_call({slot_map_updated, {ClusterMap, AddrToPid}}, _From, State) ->
    %% The idea is to store the connection pids in a tuple and then
    %% have a binary where each byte corresponds to a slot and the
    %% value maps to a index in the tuple.

    %% Create a list of indices, one for each connection pid
    Ixs = lists:seq(1, maps:size(AddrToPid)),
    %% Combine the indices with the Addresses to create a lookup from Addr -> Ix
    AddrToIx = maps:from_list(lists:zip(maps:keys(AddrToPid), Ixs)),

    Slots = create_lookup_table(ClusterMap, AddrToIx),
    Connections = create_connection_pid_tuple(AddrToPid, AddrToIx),
    {reply, ok, State#st{slots = Slots, connections = Connections}};

handle_call({command, Command, Key}, From, State) ->
    Slot = redis_lib:hash(Key),
    case binary:at(State#st.slots, Slot) of
        0 ->
            {reply, {error, unmapped_slot}, State};
        Ix ->
            Connection = element(Ix, State#st.connections),
            Fun = fun(Reply) -> gen_server:reply(From, Reply) end,
            redis_client:request_cb_raw(Connection, Command, Fun),
            {noreply, State}
    end.


handle_cast(_Request, State) ->
    {noreply, State}.


handle_info(_Info, State) ->
    {noreply, State}.


terminate(_Reason, _State) ->
    ok.


code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


format_status(_Opt, Status) ->
    Status.

%%%===================================================================
%%% Internal functions
%%%===================================================================

info_cb(Pid, Msg) ->
    case Msg of
        {slot_map_updated, ClusterSlotsInfo} ->
            gen_server:call(Pid, {slot_map_updated, ClusterSlotsInfo});
        _ ->
            ignore
    end.

create_connection_pid_tuple(AddrToPid, AddrToIx) ->
    %% Create a list with tuples where the first element is the index and the second is the pid
    IxPid = [{maps:get(Addr, AddrToIx), Pid} || {Addr, Pid} <- maps:to_list(AddrToPid)],
    %% Sort the list and remove the index to get the pids in the right order
    Pids = [Pid || {_Ix, Pid} <- lists:sort(IxPid)],
    list_to_tuple(Pids).

create_lookup_table(ClusterMap, AddrToIx) ->
    %% Replace the Addr in the slot map with the index using the lookup
    Slots = [{Start, End, maps:get(Addr,AddrToIx)} || {Start, End, Addr} <- redis_lib:parse_cluster_slots(ClusterMap)],
    create_lookup_table(0, Slots, <<>>).

create_lookup_table(16383, _, Acc) ->
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
        true  ->
            create_lookup_table(N, Rest, Acc)
    end.
