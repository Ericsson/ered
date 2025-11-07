-module(ered_cluster_convergence_SUITE).

-compile([export_all, nowarn_export_all]).

-include("ered_test_utils.hrl").
-include_lib("stdlib/include/assert.hrl").

all() ->
    [t_convergence_other_node_converges,
     t_convergence_init_node_converges].

init_per_suite(_Config) ->
    {ok, _} = application:ensure_all_started(ered, temporary),
    [].

end_per_suite(_Config) ->
    ok.

%% Check that the convergence check works when one node initially returns
%% different slot info compared to the init node and later converges with the
%% init node. Additionally, check that ered doesn't send CLUSTER SLOTS too
%% frequently.
t_convergence_other_node_converges(_Config) ->
    Host = "127.0.0.1",
    {ok, Listen1} = gen_tcp:listen(0, [binary, {packet, 0}, {active, false}]),
    {ok, Listen2} = gen_tcp:listen(0, [binary, {packet, 0}, {active, false}]),
    {ok, Port1} = inet:port(Listen1),
    {ok, Port2} = inet:port(Listen2),
    Id1 = <<"abcdef0000000000000000000000000000000001">>,
    Id2 = <<"abcdef0000000000000000000000000000000002">>,
    SlotsReply1 = [{0, 9999, Host, Port1, Id1},
                   {10000, 16383, Host, Port2, Id2}],
    SlotsReply2 = [{0, 0, Host, Port2, Id1},
                   {1, 9999, Host, Port1, Id2},
                   {10000, 16383, Host, Port2, Id1}],
    spawn_link(
      fun() ->
              %% Simulated server node 1
              {ok, Socket} = gen_tcp:accept(Listen1),
              ok = expect(Socket, <<"*2\r\n$7\r\nCLUSTER\r\n$4\r\nMYID\r\n">>),
              ok = gen_tcp:send(Socket, <<"$", (integer_to_binary(size(Id1)))/binary, "\r\n", Id1/binary, "\r\n">>),
              ok = expect(Socket, <<"*2\r\n$5\r\nHELLO\r\n$1\r\n3\r\n">>),
              ok = gen_tcp:send(Socket, encode_hello_reply()),

              ok = expect(Socket, <<"*2\r\n$7\r\nCLUSTER\r\n$5\r\nSLOTS\r\n">>),
              ClusterSlotsTime0 = erlang:monotonic_time(millisecond),
              ok = gen_tcp:send(Socket, encode_cluster_slots_reply(SlotsReply1)),

              ok = expect(Socket, <<"*2\r\n$7\r\nCLUSTER\r\n$5\r\nSLOTS\r\n">>),
              ClusterSlotsTime1 = erlang:monotonic_time(millisecond),
              ok = gen_tcp:send(Socket, encode_cluster_slots_reply(SlotsReply1)),

              {error, closed} = gen_tcp:recv(Socket, 0),

              %% Check that it was at least 100ms between the CLUSTER SLOTS.
              ?assert(ClusterSlotsTime1 >= ClusterSlotsTime0 + 100),
              ok
      end),
    spawn_link(
      fun() ->
              %% Simulated server node 2
              {ok, Socket} = gen_tcp:accept(Listen2),
              ok = expect(Socket, <<"*2\r\n$7\r\nCLUSTER\r\n$4\r\nMYID\r\n">>),
              ok = gen_tcp:send(Socket, <<"$", (integer_to_binary(size(Id2)))/binary, "\r\n", Id2/binary, "\r\n">>),
              ok = expect(Socket, <<"*2\r\n$5\r\nHELLO\r\n$1\r\n3\r\n">>),
              ok = gen_tcp:send(Socket, encode_hello_reply()),

              ok = expect(Socket, <<"*2\r\n$7\r\nCLUSTER\r\n$5\r\nSLOTS\r\n">>),
              ClusterSlotsTime0 = erlang:monotonic_time(millisecond),
              ok = gen_tcp:send(Socket, encode_cluster_slots_reply(SlotsReply2)),

              ok = expect(Socket, <<"*2\r\n$7\r\nCLUSTER\r\n$5\r\nSLOTS\r\n">>),
              ClusterSlotsTime1 = erlang:monotonic_time(millisecond),
              ok = gen_tcp:send(Socket, encode_cluster_slots_reply(SlotsReply1)),

              {error, closed} = gen_tcp:recv(Socket, 0),

              %% Check that it was at least 100ms between the CLUSTER SLOTS.
              ?assert(ClusterSlotsTime1 >= ClusterSlotsTime0 + 100),
              ok
      end),
    {ok, ClusterPid} = ered_cluster:connect([{Host, Port1}],
                                            [{info_pid, [self()]},
                                             {client_opts,
                                              [{connection_opts, [{connect_timeout, 100}]}]}]),
    ?MSG(#{msg_type := connected, addr := {Host, Port1}, cluster_id := Id1}),
    ?MSG(#{msg_type := slot_map_updated, addr := {Host, Port1}}),
    ?MSG(#{msg_type := connected, addr := {Host, Port2}, cluster_id := Id2}),
    ?MSG(#{msg_type := cluster_ok}, 3000),

    %% No more messages
    receive X -> error({unexpected, X}) after 0 -> ok end,
    ok = ered_cluster:close(ClusterPid),
    ok = gen_tcp:close(Listen1),
    ok = gen_tcp:close(Listen2),
    ok.

%% Check that the convergence check works when the init node initially returns
%% different slot info compared to the other node and later changes and
%% converges with the other node. Additionally, check that ered doesn't send
%% CLUSTER SLOTS too frequently.
t_convergence_init_node_converges(_Config) ->
    Host = "127.0.0.1",
    {ok, Listen1} = gen_tcp:listen(0, [binary, {packet, 0}, {active, false}]),
    {ok, Listen2} = gen_tcp:listen(0, [binary, {packet, 0}, {active, false}]),
    {ok, Port1} = inet:port(Listen1),
    {ok, Port2} = inet:port(Listen2),
    Id1 = <<"abcdef0000000000000000000000000000000001">>,
    Id2 = <<"abcdef0000000000000000000000000000000002">>,
    SlotsReply1 = [{0, 9999, Host, Port1, Id1},
                   {10000, 16383, Host, Port2, Id2}],
    SlotsReply2 = [{0, 0, Host, Port2, Id1},
                   {1, 9999, Host, Port1, Id2},
                   {10000, 16383, Host, Port2, Id1}],
    spawn_link(
      fun() ->
              %% Simulated server node 1
              {ok, Socket} = gen_tcp:accept(Listen1),
              ok = expect(Socket, <<"*2\r\n$7\r\nCLUSTER\r\n$4\r\nMYID\r\n">>),
              ok = gen_tcp:send(Socket, <<"$", (integer_to_binary(size(Id1)))/binary, "\r\n", Id1/binary, "\r\n">>),
              ok = expect(Socket, <<"*2\r\n$5\r\nHELLO\r\n$1\r\n3\r\n">>),
              ok = gen_tcp:send(Socket, encode_hello_reply()),

              ok = expect(Socket, <<"*2\r\n$7\r\nCLUSTER\r\n$5\r\nSLOTS\r\n">>),
              ClusterSlotsTime0 = erlang:monotonic_time(millisecond),
              ok = gen_tcp:send(Socket, encode_cluster_slots_reply(SlotsReply2)),

              ok = expect(Socket, <<"*2\r\n$7\r\nCLUSTER\r\n$5\r\nSLOTS\r\n">>),
              ClusterSlotsTime1 = erlang:monotonic_time(millisecond),
              ok = gen_tcp:send(Socket, <<"-LOADING Valkey is loading the dataset in memory\r\n">>),

              ok = expect(Socket, <<"*2\r\n$7\r\nCLUSTER\r\n$5\r\nSLOTS\r\n">>),
              ClusterSlotsTime2 = erlang:monotonic_time(millisecond),
              ok = gen_tcp:send(Socket, encode_cluster_slots_reply(SlotsReply1)),

              {error, closed} = gen_tcp:recv(Socket, 0),

              %% Check that there was some time between each CLUSTER SLOTS.
              ?assert(ClusterSlotsTime1 >= ClusterSlotsTime0 + 100),
              ?assert(ClusterSlotsTime2 >= ClusterSlotsTime1 + 100),
              ok
      end),
    spawn_link(
      fun() ->
              %% Simulated server node 2
              {ok, Socket} = gen_tcp:accept(Listen2),
              ok = expect(Socket, <<"*2\r\n$7\r\nCLUSTER\r\n$4\r\nMYID\r\n">>),
              ok = gen_tcp:send(Socket, <<"$", (integer_to_binary(size(Id2)))/binary, "\r\n", Id2/binary, "\r\n">>),
              ok = expect(Socket, <<"*2\r\n$5\r\nHELLO\r\n$1\r\n3\r\n">>),
              ok = gen_tcp:send(Socket, encode_hello_reply()),

              ok = expect(Socket, <<"*2\r\n$7\r\nCLUSTER\r\n$5\r\nSLOTS\r\n">>),
              ClusterSlotsTime0 = erlang:monotonic_time(millisecond),
              ok = gen_tcp:send(Socket, encode_cluster_slots_reply(SlotsReply1)),

              ok = expect(Socket, <<"*2\r\n$7\r\nCLUSTER\r\n$5\r\nSLOTS\r\n">>),
              ClusterSlotsTime1 = erlang:monotonic_time(millisecond),
              ok = gen_tcp:send(Socket, encode_cluster_slots_reply(SlotsReply1)),

              {error, closed} = gen_tcp:recv(Socket, 0),

              %% Check that there was some time between each CLUSTER SLOTS.
              ?assert(ClusterSlotsTime1 >= ClusterSlotsTime0 + 100),
              ok
      end),
    {ok, ClusterPid} = ered_cluster:connect([{Host, Port1}],
                                            [{info_pid, [self()]},
                                             {client_opts,
                                              [{connection_opts, [{connect_timeout, 100}]}]}]),
    ?MSG(#{msg_type := connected, addr := {Host, Port1}, cluster_id := Id1}),
    ?MSG(#{msg_type := slot_map_updated, addr := {Host, Port1}}),
    ?MSG(#{msg_type := connected, addr := {Host, Port2}, cluster_id := Id2}),
    ?MSG(#{msg_type := cluster_slots_error_response, addr := {Host, Port1},
           response := <<"LOADING", _/binary>>}, 3000),
    ?MSG(#{msg_type := slot_map_updated}, 3000), % Can come from either node.
    ?MSG(#{msg_type := cluster_ok}, 3000),

    %% No more messages
    receive X -> error({unexpected, X}) after 0 -> ok end,
    ok = ered_cluster:close(ClusterPid),
    ok = gen_tcp:close(Listen1),
    ok = gen_tcp:close(Listen2),
    ok.

%% Read the expected data from the socket. Returns an error if the data from the
%% socket doesn't match the expected data.
expect(Socket, Expected) when is_binary(Expected) ->
    case gen_tcp:recv(Socket, size(Expected)) of
        {ok, Expected} ->
            ok;
        {ok, Unexpected} ->
            {error, {unexpected, Unexpected}};
        {error, Reason} ->
            {error, Reason}
    end.

encode_hello_reply() ->
    <<"%7\r\n",
      "$6\r\nserver\r\n", "$6\r\nvalkey\r\n",
      "$7\r\nversion\r\n", "$11\r\n255.255.255\r\n",
      "$5\r\nproto\r\n", ":3\r\n"
      "$2\r\nid\r\n", ":2\r\n",
      "$4\r\nmode\r\n", "$7\r\ncluster\r\n"
      "$4\r\nrole\r\n", "$6\r\nmaster\r\n"
      "$7\r\nmodules\r\n", "*0\r\n">>.

encode_cluster_slots_reply(Nodes) ->
    Prefix = <<"*", (integer_to_binary(length(Nodes)))/binary, "\r\n">>,
    lists:foldl(fun append_cluster_slots_row/2, Prefix, Nodes).

append_cluster_slots_row({Start, End, Host, Port, NodeId}, AccBin) ->
    HostBin = list_to_binary(Host),
    <<AccBin/binary,
      "*3\r\n",
      ":", (integer_to_binary(Start))/binary, "\r\n",
      ":", (integer_to_binary(End))/binary, "\r\n",
      "*4\r\n",
      "$", (integer_to_binary(size(HostBin)))/binary, "\r\n", HostBin/binary, "\r\n",
      ":", (integer_to_binary(Port))/binary, "\r\n",
      "$", (integer_to_binary(size(NodeId)))/binary, "\r\n", NodeId/binary, "\r\n",
      "*0\r\n">>.
