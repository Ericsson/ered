-module(ered_info_msg).

%% Functions used to format and send info messages

-export([connection_status/3,
         slot_map_updated/4,
         cluster_slots_error_response/3,
         cluster_ok/1,
         cluster_nok/2,
         cluster_stopped/2
        ]).


-export_type([info_msg/0]).

%%%===================================================================
%%% Definitions
%%%===================================================================

-type info_msg() ::
        ered_client:info_msg() |

        #{msg_type := slot_map_updated,
          slot_map := ClusterSlotsReply :: any(),
          addr := addr(),
          map_version := non_neg_integer()} |

        #{msg_type := cluster_slots_error_response,
          addr := addr(),
          response := RedisReply :: any()} |

        #{msg_type := cluster_ok} |

        #{msg_type := cluster_not_ok,
          reason := master_down | master_queue_full | pending | not_all_slots_covered | too_few_replicas} |

        #{msg_type := cluster_stopped,
          reason := any()}.


-type addr() :: ered:addr().

%%%===================================================================
%%% API
%%%===================================================================

%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-spec connection_status(ered_client:info_msg(), boolean(), [pid()]) -> ok.
%%
%% Client connection goes up or down.
%% Client queue full or queue recovered to OK level.
%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
connection_status(Msg, IsMaster, Pids) ->
    send_info(Msg#{master => IsMaster}, Pids).

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

%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-spec cluster_stopped([pid()], any()) -> ok.
%%
%% The cluster instance is terminating.
%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
cluster_stopped(Pids, Reason) ->
    send_info(#{msg_type => cluster_stopped, reason => Reason}, Pids).

%%%===================================================================
%%% Internal functions
%%%===================================================================

send_info(Msg, Pids) ->
    [Pid ! Msg || Pid <- Pids],
    ok.

