
-module(redis).

-behaviour(gen_server).

%% API
-export([start_link/3]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3, format_status/2]).


-record(st, {cluster_pid :: pid()}).

%%%===================================================================
%%% API
%%%===================================================================
start_link(Host, Port, Opts) ->
    gen_server:start_link(?MODULE, [Host, Port, Opts], []).

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
    {ok, #st{cluster_pid = ClusterPid}}.


handle_call({slot_map_updated, {ClusterMap, AddrToPid}}, _From, State) ->
    ClusterSlots = redis_lib:parse_cluster_slots(ClusterMap),
    AddrToIx = maps:from_list(lists:zip(maps:keys(AddrToPid), lists:seq(1, maps:size(AddrToPid)))),
    Slots = [{Start, End, maps:get(Addr,AddrToIx)} || {Start, End, Addr} <-  ClusterSlots],

    Pids = list_to_tuple([Pid || {_Ix, Pid} <- lists:sort([{maps:get(Addr, AddrToIx), Pid} || {Addr, Pid} <- maps:to_list(AddrToPid)])]),
    ok = {create_b(0, Slots, <<>>), Pids},
    Reply = ok,
    {reply, Reply, State}.


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

create_b(16383, _, Acc) ->
    Acc;
create_b(N, [], Acc) ->
    create_b(N+1, [], <<Acc/binary,0>>);
create_b(N, L = [{Start, End, Val} | Rest], Acc) ->
    if
        N < Start -> % unmapped, use 0
            create_b(N+1, L, <<Acc/binary,0>>);
        N =< End -> % in range
            create_b(N+1, L, <<Acc/binary,Val>>);
        true  ->
            create_b(N, Rest, Acc)
    end.
