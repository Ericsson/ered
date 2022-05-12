-module(redis_info_msg).

-export([connection_status/3,
         slot_map_updated/3,
         cluster_slots_error_response/2,
         cluster_ok/1,
         cluster_nok/2
        ]).


-export_type([info_msg/0]).

-type node_info(MsgType, Reason) ::
        #{msg_type := MsgType,
          reason := Reason,
          master := boolean(),
          addr := addr(),
          client_id := pid(),
          node_id := string()
         }.

-type info_msg() ::
        node_info(connected, none) |

        node_info(socket_closed, any()) |

        node_info(connect_error, any()) |

        node_info(init_error, any()) |

        node_info(queue_ok, none) |

        node_info(queue_full, none) |

        node_info(client_stopped, any()) |

        #{msg_type := slot_map_updated,
          slot_map := ClusterSlotsReply :: any(),
          map_version := non_neg_integer()} |

        #{msg_type := cluster_slots_error_response,
          response := RedisReply :: any()} |

        #{msg_type := cluster_ok} |

        #{msg_type := cluster_not_ok,
          reason := master_down | master_node_queue_full | bad_slot_map}.


-type addr() :: redis_client:addr().

%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-spec connection_status(redis_client:info_msg(), boolean(), [pid()]) -> ok.
%%
%% Client connection goes up or down.
%% Client queue full or queue recovered to OK level.
%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
connection_status(ClientInfo, IsMaster, Pids) ->
    {connection_status, {Pid, Addr, Id} , Status} =  ClientInfo,
    {MsgType, Reason} =
        case Status of
            connection_up -> {connected, ok};
            {connection_down, R} -> R;
            queue_full -> {queue_full, ok};
            queue_ok -> {queue_ok, ok};
            {socket_closed, R} -> {socket_closed, R}
        end,
    send_info(#{msg_type => MsgType,
                reason => Reason,
                master => IsMaster,
                addr => Addr,
                client_id => Pid,
                cluster_id => Id},
              Pids).

%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-spec slot_map_updated(redis_lib:slot_map(), non_neg_integer(), [pid()]) -> ok.
%%
%% A new slot map received from Redis, different from the current one.
%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
slot_map_updated(ClusterSlotsReply, Version, Pids) ->
    send_info(#{msg_type => slot_map_updated,
                slot_map => ClusterSlotsReply,
                map_version => Version},
              Pids).


%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-spec cluster_slots_error_response(binary(), [pid()]) -> ok.
%%
%% Redis returned and error message when trying to fetch the slot map.
%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
cluster_slots_error_response(Response, Pids) ->
    send_info(#{msg_type => cluster_slots_error_response,
                response => Response},
              Pids).

%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-spec cluster_ok([pid()]) -> ok.
%%
%% All clients to master nodes are up and their queues are OK
%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
cluster_ok(Pids) ->
    send_info(#{msg_type => cluster_ok},
              Pids).

%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-spec cluster_nok(master_down |
                  master_queue_full |
                  too_few_nodes |
                  not_all_slots_covered |
                  too_few_replicas,
                  [pid()]) -> ok.
%%
%% There is a problem with the cluster, requests are not guaranteed
%% to be served.
%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
cluster_nok(Reason, Pids) ->
    send_info(#{msg_type => cluster_not_ok,
                reason => Reason},
              Pids).

%%%===================================================================
%%% Internal functions
%%%===================================================================

send_info(Msg, Pids) ->
    [Pid ! Msg || Pid <- Pids],
    ok.

