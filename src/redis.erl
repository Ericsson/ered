
-module(redis).

-behaviour(gen_server).

%% API
-export([start_link/2,
         stop/1,
         command/3, command/4,
         command_all/2, command_all/3,
         command_client/2, command_client/3, command_client_cb/3,
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

%%%===================================================================
%%% API
%%%===================================================================
start_link(Addrs, Opts) ->
    gen_server:start_link(?MODULE, [Addrs, Opts], []).

stop(ServerRef) ->
    gen_server:stop(ServerRef).

command(ServerRef, Command, Key) ->
    command(ServerRef, Command, Key, infinity).

command(ServerRef, Command, Key, Timeout) ->
    C = redis_lib:format_request(Command),
    gen_server:call(ServerRef, {command, C, Key}, Timeout).

command_all(ServerRef, Command) ->
    command_all(ServerRef, Command, infinity).

command_all(ServerRef, Command, Timeout) ->
    %% Send command in sequence to all instances.
    %% This could be done in parallel but but keeping it easy and
    %% aligned with eredis_cluster for now
    Cmd = redis_lib:format_request(Command),
    [redis_client:request(ClientRef, Cmd, Timeout) || ClientRef <- get_clients(ServerRef)].

command_client(ClientRef, Command) ->
    command_client(ClientRef, Command, infinity).

command_client(ClientRef, Command, Timeout) ->
    redis_client:request(ClientRef, Command, Timeout).

command_client_cb(ClientRef, Command, CallbackFun) ->
    redis_client:request_cb(ClientRef, Command, CallbackFun).

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
    redis_client:request_cb(Client, Command, Fun),
    {noreply, State1};

handle_cast({forward_command_asking, Command, Slot, From, Addr, AttemptsLeft, OldReply}, State) ->
    {Client, State1} = connect_addr(Addr, State),
    Command1 = add_asking(OldReply, Command),
    HandleReplyFun = create_reply_fun(Command, Slot, Client, From, State, AttemptsLeft),
    %% Fun = case is_single_command(Command) of
    %%           true ->
    %%               fun(Reply) -> HandleReplyFun(fix_ask_reply_single(Reply)) end;
    %%           false ->
    %%               fun(Reply) -> HandleReplyFun(remove_asking_ok_reply(Reply)) end
    %%       end,
    %Fun = fun(Reply) -> HandleReplyFun(fix_ask_reply(OldReply, Reply)) end,

    Fun = fun(Reply) -> 
                  Reply2 = fix_ask_reply(OldReply, Reply),
                  io:format("asking_reply_fun, ~p\n", [{OldReply, Reply, Reply2}]),
                  HandleReplyFun(Reply2)
          end,

    io:format("forward_command_asking, ~p\n", [{Command, OldReply, Command1}]),
    redis_client:request_cb(Client, Command1, Fun),
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
            redis_client:request_cb(Client, Command, Fun)
    end,
    ok.


create_reply_fun(_Command, _Slot, _Client, From, _State, 0) ->
    fun(Reply) -> gen_server:reply(From, Reply) end;

create_reply_fun(Command, Slot, Client, From, State, AttemptsLeft) ->
    Pid = self(),
    fun(Reply) ->
            %% TODO remove
            io:format("~p\n", [{AttemptsLeft, Command, Reply}]),
            case check_result(Reply) of
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


check_result({ok, Result}) when is_list(Result) ->
    check_for_special_reply(Result);
check_result({ok, Result}) ->
    check_for_special_reply([Result]);
check_result(_) ->
    normal.

check_for_special_reply([]) ->
    normal;
check_for_special_reply([Head|Tail]) ->
    case Head of
        {error, <<"TRYAGAIN ", _/binary>>} ->
            try_again;
        {error, <<"MOVED ", Bin/binary>>} ->
            {moved, parse_host_and_port(Bin)};
        {error, <<"ASK ", Bin/binary>>} ->
            {ask, parse_host_and_port(Bin)};
        _ ->
            check_for_special_reply(Tail)
    end.

parse_host_and_port(Bin) ->
    %% looks like <<"MOVED 14039 127.0.0.1:30006">> or <<"ASK 15118 127.0.0.1:30002">>
    [_Slot, AddrBin] = binary:split(Bin, <<" ">>),
    %% Use trailing, IPv6 address can contain ':'
    [Host, Port] = string:split(AddrBin, <<":">>, trailing),
    {binary_to_list(Host), binary_to_integer(Port)}.


%% special_reply({ok, [Head|_Tail]}) ->
%%     %% Only consider the first element in the pipeline. A pipeline "should" not
%%     %% contain multiple keys. Setting or reading multiple keys is hopefully done
%%     %% with some multi operation like MGET/MSET. If the first command is some
%%     %% keyless comand then there would be trouble though.. I dont think its worth
%%     %% to make this more complicated and/or costly unless there is a good
%%     %% real world use case for it and right now I don't have it.
%%     special_reply({ok, Head});

%% special_reply({ok, {error, <<"MOVED ", Tail/binary>>}}) ->
%%     %% looks like this ,<<"MOVED 14039 127.0.0.1:30006">>
%%     %% TODO should this be wrapped in a try catch if MOVED is malformed?
%%     [_Slot, AddrBin] = binary:split(Tail, <<" ">>),
%%     %% Use trailing, IPv6 address can contain ':'
%%     [Host, Port] = string:split(AddrBin, <<":">>, trailing),
%%     {moved, {binary_to_list(Host), binary_to_integer(Port)}};

%% special_reply({ok, {error, <<"ASK ", Tail/binary>>}}) ->
%%     %% Similar format to move
%%     [_Slot, AddrBin] = binary:split(Tail, <<" ">>),
%%     [Host, Port] = string:split(AddrBin, <<":">>, trailing),
%%     {ask, {binary_to_list(Host), binary_to_integer(Port)}};

%% special_reply({ok, {error, <<"TRYAGAIN ", _/binary>>}}) ->
%%     try_again;

%% special_reply(_) ->
%%     normal.


connect_addr(Addr, State) ->
    case maps:get(Addr, State#st.addr_map, not_found) of
        not_found ->
            Client = redis_cluster2:connect_node(State#st.cluster_pid, Addr),
            {Client, State#st{addr_map = maps:put(Addr, Client, State#st.addr_map)}};
        Client ->
            {Client, State}
    end.


add_asking(_, {redis_command, single, Command}) ->
    {redis_command, 2, [ <<"ASKING\r\n">>, Command]};

add_asking({ok, OldReplies}, {redis_command, _N, Commands}) ->
    %% Extract only the commands with ASK redirection
    AskCommands = add_asking_pipeline(OldReplies, Commands),
    %% ASK commands + ASKING
    N = length(AskCommands) * 2,
    {redis_command, N, AskCommands}.

add_asking_pipeline([], _) ->
    [];
add_asking_pipeline([{error, <<"ASK ", _/binary>>} |Replies], [Command |Commands]) ->
    [[ <<"ASKING\r\n">>, Command] | add_asking_pipeline(Replies, Commands)];
add_asking_pipeline([_ |Replies], [_ |Commands]) ->
    add_asking_pipeline(Replies, Commands).


fix_ask_reply({ok, OldReplies}, {ok, NewReplies}) when is_list(OldReplies) ->
    {ok, merge_ask_result(OldReplies, NewReplies)};
fix_ask_reply(_OldReply, {ok, [_AskOk, Reply]}) ->
    {ok, Reply};
fix_ask_reply(_, Other) ->
    Other.


%% fix_ask_reply_single({ok, [_AskOk, Reply]}) ->
%%     {ok, Reply};
%% fix_ask_reply_single(Other) ->
%%     Other.

%% fix_ask_reply_pipeline({ok, OldReplies}, {ok, AskReplies}) ->
%%     {ok, merge_ask_result([], [])};
%% fix_ask_reply_pipeline(_, Other) ->
%%     Other.


merge_ask_result([], _) ->
    [];
merge_ask_result([{error, <<"ASK ", _/binary>>} | Replies1], [_AskOk, Reply | Replies2]) ->
    [Reply | merge_ask_result(Replies1, Replies2)];
merge_ask_result([Reply | Replies1], Replies2) ->
    [Reply | merge_ask_result(Replies1, Replies2)].


%% add_asking({redis_command, N, Commands}) ->
%%     case N of
%%         single ->
%%             {redis_command, 2, [ <<"ASKING\r\n">>, Commands]};
%%         _ ->
%%             {redis_command, N*2, [[ <<"ASKING\r\n">>, Command] || Command<- Commands]}
%%     end.

%% is_single_command({redis_command, N, _Commands}) ->
%%     N == single.

%% remove_asking_ok_reply(Reply) ->
%%     case Reply of
%%         {ok, Results} ->
%%             {ok, remove_even_ix_element(Results)};
%%         Other ->
%%             Other
%%     end.

%% remove_even_ix_element(Ls) ->
%%     case Ls of
%%         [] ->
%%             [];
%%         [_, X|Tail] ->
%%             [X | remove_even_ix_element(Tail)]
%%     end.

%% remove_asking_ok_reply_single(Reply) ->
%%     case Reply of
%%         {ok, [_Ok, Result]} ->
%%             {ok, Result};
%%         Other ->
%%             Other
%%     end.

take_prop(Key, List, Default) ->
    Val = proplists:get_value(Key, List, Default),
    NewList = proplists:delete(Key, List),
    {Val, NewList}.


 
