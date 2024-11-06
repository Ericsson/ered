-module(ered_info_msg).

%% Functions used to format and send info messages

-export([connection_status/3,
         slot_map_updated/4,
         cluster_slots_error_response/3,
         cluster_ok/1,
         cluster_nok/2
        ]).


-export_type([info_msg/0]).

%%%===================================================================
%%% Definitions
%%%===================================================================

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

        node_info(node_down_timeout, none) |

        node_info(node_deactivated, none) |

        node_info(queue_ok, none) |

        node_info(queue_full, none) |

        node_info(client_stopped, any()) |

        #{msg_type := slot_map_updated,
          slot_map := ClusterSlotsReply :: any(),
          addr := addr(),
          map_version := non_neg_integer()} |

        #{msg_type := cluster_slots_error_response,
          addr := addr(),
          response := RedisReply :: any()} |

        #{msg_type := cluster_ok} |

        #{msg_type := cluster_not_ok,
          reason := master_down | master_queue_full | pending | not_all_slots_covered | too_few_replicas}.


-type addr() :: ered_client:addr().

%%%===================================================================
%%% API
%%%===================================================================

%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-spec connection_status(ered_client:info_msg(), boolean(), [pid()]) -> ok.
%%
%% Client connection goes up or down.
%% Client queue full or queue recovered to OK level.
%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
connection_status(ClientInfo, IsMaster, Pids) ->
    {connection_status, {Pid, Addr, Id} , Status} =  ClientInfo,
    {MsgType, Reason} =
        case Status of
            connection_up                        -> {connected, none};
            {connection_down, R} when is_atom(R) -> {R, none};
            {connection_down, R}                 -> R;
            node_deactivated                     -> {node_deactivated, none};
            queue_full                           -> {queue_full, none};
            queue_ok                             -> {queue_ok, none}
        end,
    send_info(#{msg_type => MsgType,
                reason => Reason,
                master => IsMaster,
                addr => Addr,
                client_id => Pid,
                cluster_id => Id},
              Pids).

%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-spec slot_map_updated(ered_lib:slot_map(), non_neg_integer(), addr(), [pid()]) -> ok.
%%
%% A new slot map received from Redis, different from the current one.
%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
slot_map_updated(ClusterSlotsReply, Version, FromAddr, Pids) ->
    send_info(#{msg_type => slot_map_updated,
                slot_map => ClusterSlotsReply,
                addr => FromAddr,
                map_version => Version},
              Pids).


%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-spec cluster_slots_error_response(binary() | empty, addr(), [pid()]) -> ok.
%%
%% Redis returned an error message when trying to fetch the slot map.
%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
cluster_slots_error_response(Response, FromAddr, Pids) ->
    send_info(#{msg_type => cluster_slots_error_response,
                addr => FromAddr,
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
                  pending |
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

