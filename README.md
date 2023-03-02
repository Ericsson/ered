ered
====

An Erlang client library for connecting to Redis Cluster
aiming to replace [eredis](https://github.com/Nordix/eredis) and [eredis_cluster](https://github.com/Nordix/eredis_cluster).

Status: Beta.

Features:

* status events
* queuing and load shedding (to handle burst traffic)
* pipelining of commands
* [RESP3](https://github.com/redis/redis-specifications/blob/master/protocol/RESP3.md) support
* ASK redirection supporting hash tags

Usage by example
----------------

```Erlang
1> {ok, Pid} = ered:start_link([{"localhost", 6379}], []).
{ok,<0.164.0>}
2> ered:command(Pid, [<<"SET">>, <<"mykey">>, <<"42">>], <<"mykey">>, 5000).
{ok,<<"OK">>}
3> ered:command_async(Pid, [<<"GET">>, <<"mykey">>], <<"mykey">>, fun(Reply) -> io:format("Reply: ~p~n", [Reply]) end).
ok
Reply: {ok,<<"42">>}
4> ered:stop(Pid).
ok
```

Functions
---------

### `start_link/2`

```Erlang
start_link([addr()], [opt()]) -> {ok, server_ref()} | {error, term()}.
```

Start the main process. This will also start the cluster handling
process which will set up clients to the provided addresses and
fetch the cluster slot map. Once there is a complete slot map and
all Redis node clients are connected this process is ready to
serve requests.

One or more addresses, `addr() :: {inet:socket_address() | inet:hostname(),
inet:port_number()}`, is used to discover the rest of the cluster.

For options, see [Options](#options) below.

### `stop/1`

```Erlang
stop(server_ref()) -> ok.
```

Stop the main process. This will also stop the cluster handling
process and in turn disconnect and stop all clients.

### `command/3,4`

```Erlang
command(server_ref(), command(), key()) -> reply().
command(server_ref(), command(), key(), timeout()) -> reply().
```

Send a command to the Redis cluster. The command will be routed to
the correct Redis node client based on the provided key.
If the command is a single command then it is represented as a
list of binaries where the first binary is the Redis command
to execute and the rest of the binaries are the arguments.
If the command is a pipeline, e.g. multiple commands to executed
then they need to all map to the same slot for things to
work as expected.

`command/3` is the same as setting the timeout to infinity.

### `command_async/4`

```Erlang
command_async(server_ref(), command(), key(), fun((reply()) -> any())) -> ok.
```

Like command/3,4 but asynchronous. Instead of returning the reply, the reply
function is applied to the reply when it is available. The reply function
runs in an unspecified process.

### `command_all/2,3`

```Erlang
command_all(server_ref(), command()) -> [reply()].
command_all(server_ref(), command(), timeout()) -> [reply()].
```

Send the same command to all connected master Redis nodes.

### `command_client/2,3`

```Erlang
command_client(client_ref(), command()) -> reply().
command_client(client_ref(), command(), timeout()) -> reply().
```

Send the command to a specific Redis client without any client routing.

### `command_client_async/3`

```Erlang
command_client_async(client_ref(), command(), reply_fun()) -> ok.
```

Send command to a specific Redis client in asynchronous fashion. The
provided callback function will be called with the reply. Note that
the callback function will executing in the redis client process and
should not hang or perform any lengthy task.

### `get_clients/1`

```Erlang
get_clients(server_ref()) -> [client_ref()].
```

Get all Redis master node clients.

### `get_addr_to_client_map/1`

```Erlang
get_addr_to_client_map(server_ref()) -> #{addr() => client_ref()}.
```

Get the address to client mapping. This includes all clients.

### `update_slots/1,2`

```Erlang
update_slots(server_ref()) -> ok.
update_slots(server_ref(), client_ref()) -> ok.
```

Manually trigger a slot mapping update. If a client pid or name is provided and
available, this client is used to fetch the slot map. Otherwise a random node is
used.

Options
-------

The following options can be passed to `start_link/2`:

* `{try_again_delay, non_neg_integer()}`

  If there is a TRYAGAIN response from Redis then wait this many milliseconds
  before re-sending the command. Default 200.

* `{redirect_attempts, non_neg_integer()}`

  Only do these many retries or re-sends before giving up and returning the
  result. This affects ASK, MOVED and TRYAGAIN responses. Default 10.

* `{info_pid, [pid()]}`

  List of pids to receive cluster info messages. See [Info
  messages](#info-messages) below.

* `{update_slot_wait, non_neg_integer()}`

  CLUSTER SLOTS command is used to fetch slots from the Redis cluster. This
  value sets how long in milliseconds to wait before trying to send the command
  again. Default 500.

* `{client_opts, [ered_client:opt()]}`

  Options passed to each client. See [Client options](#client-options) below.

* `{min_replicas, non_neg_integer()}`

  For each Redis master node, the min number of replicas for the cluster
  to be considered OK. Default 1.

* `{close_wait, non_neg_integer()}`

  How long to delay the closing of clients that are no longer part of the slot
  map, in milliseconds. The delay is needed so that messages sent to the client
  are not lost in transit. Although the closing of these clients is delayed, the
  clients are put in a state so they respond immediately to commands with
  `{error, node_deactivated}`. Default 10000.

### Client options

Options passed to `start_link/2` as the options `{client_opts, [...]}`.

* `{connection_opts, [ered_connection:opt()]}`

  Options passed to the connection module. See [Connection options](#connection-options) below.

* `{max_waiting, non_neg_integer()}`

  Max number of commands allowed to wait in queue. Default 5000.

* `{max_pending, non_neg_integer()}`

  Max number of commands to be pending, i.e. sent to client
  and waiting for a response. Default 128.

* `{queue_ok_level, non_neg_integer()}`

  If the queue has been full then it is considered ok
  again when it reaches this level. Default 2000.

* `{reconnect_wait, non_neg_integer()}`

  How long to wait to reconnect after a failed connect attempt. Default 1000.

* `{info_pid, none | pid()}`

  Pid to send status messages to.

* `{resp_version, 2..3}`

  What RESP (REdis Serialization Protocol) version to use. Default 3.

* `{node_down_timeout, non_neg_integer()}`

  If there is a connection problem, such as a timeout waiting for a response
  from Redis, the ered client tries to set up a new connection. If the
  connection is not recovered within `node_down_timeout`, then the client
  considers the node down and clears it's queue (commands get an error reply)
  and starts rejecting all new commands until the connection is restored.
  Default 2000.

* `{use_cluster_id, boolean()}`

  Set to true if the CLUSTER ID should be fetched and used in info messages.
  This is set to true automatically when a client is started as part of a
  cluster client and false otherwise. (Not useful if the client is used outside
  of a cluster.)

### Connection options

Options passed to `start_link/2` as the options `{client_opts, [{connection_opts, [...]}]}`.

* `{batch_size, non_neg_integer()}`

  If commands are queued up in the process message queue, this is the maximum
  number of messages that will be received and sent in one call. Default 16.

* `{tcp_options, [gen_tcp:connect_option()]}`

  Options passed to `gen_tcp:connect/4`.

* `{tcp_connect_timeout, timeout()}`

  Timeout passed to gen_tcp:connect/4. Default infinity.

* `{tls_options, [ssl:tls_client_option()]}`

  Options passed to `ssl:connect/3`. If this config parameter is present
  TLS will be used.

* `{tls_connect_timeout, timeout()}`

  Timeout passed to ssl:connect/3. Default infinity.

* `{push_cb, push_cb()}`

  Callback for push notifications.

* `{response_timeout, non_neg_integer()}`

  Timeout when waiting for a response from Redis in milliseconds. Default 10000.

  When a timeout happens, the connection is closed and the client attempts to
  set up a new connection. See the client option `node_down_timeout` above.

Info messages
-------------

When one or more pids have been provided as the option `{info_pid, [pid()]}` to
`start_link/2`, these are the messages ered sends. All messages are maps with at
least the key `msg_type`.

Messages about the cluster as a whole:

* `#{msg_type := cluster_ok}` is sent when the cluster is up and runnning and
  ered is connected to all nodes.

* `#{msg_type := cluster_not_ok, reason := Reason}` is sent when something is
  wrong, where Reason is one of the following:

  * `master_down` if one of the master nodes is down or unreachable.

  * `master_queue_full` if one of the master nodes is so busy that the maximum
    number of queued commands for the node is reached and any further command to
    that node would be rejected immediately.

  * `pending` if ered has not yet established connections to all nodes.

  * `too_few_nodes` if the cluster has fewer than two nodes.

  * `not_all_slots_covered` if some cluster slot doesn't belong to any node in
    the cluster.

  * `too_few_replicas` if any of the master nodes has fewer replicas than the
    minimum number as specified using the option `{min_replicas,
    non_neg_integer()`. See options above.

* `#{msg_type := slot_map_updated, slot_map := list(), map_version :=
  non_neg_integer()}` is sent when the cluster slot-to-node mapping has been
  updated.

* `#{msg_type := cluster_slots_error_response, response := any()}` is sent when
  there was an error updating the cluster slot-to-node mapping.

Messages about the connection to a specific node are in the following form:

```Erlang
#{msg_type := MsgType,
  reason := Reason,
  master := boolean(),
  addr := addr(),
  client_id := pid(),
  node_id := string()}
```

The field `msg_type` identifies what kind of event has happened and is described
below. Reason depends on `msg_type`. Master describes whether the node is a
master or a replica. Addr is a tuple `{Host, Port}` to the Redis node. Client id
is the pid of the `ered_client` responsible for the connection. Node id is
assigned by Redis and is used to identify the node within the cluster.

The possible values of the `msg_type` field for connection events are as
follows:

* `connected` when the connection to a node has been established. Reason is
  `none`.

* `connect_error` when connecting to the node fails or the TLS handshake fails.
  Reason is as returned by `gen_tcp:connect/4` or `{tls_init, TlsReason}` if
  `ssl:connect/3` fails.

* `init_error` when one of the initial commands sent to Redis has failed, such
  as the HELLO command. Reason is a list of error reasons.

* `socket_closed` when the connection has been closed, either by the peer or by
  ered when an error has happened. Reason is `{recv_exit, RecvReason}` if
  `gen_tcp:recv/3` or `ssl:recv/3` has failed. RecvReason is typically `timeout`
  or one of the `inet:posix()` errors. Reason is `{send_exit, SendReason}` if
  `gen_tcp:send/2` or `ssl:send/2` has failed.

* `node_down_timeout` when the connection to a node is lost and the connection
  has not been re-established within the specified time. Reason is `none`.

* `node_deactivated` when the node is no longer part of the cluster but the
  client has not yet been stopped. Reason is `none`.

* `client_stopped` when a connection has been terminated, either because the
  node is no longer part of the cluster or because the user is stopping ered.
  Reason is the terminate reason of the `ered_client` process, typically the
  atom `normal`.

* `queue_full` when the command queue to the node is full. Reason `none`.

* `queue_ok` when the command queue to the node is not full anymore. Reason is
  `none`.

Redis to Erlang Term Representation
-----------------------------------

| Redis (RESP3)         | Erlang                                             |
|-----------------------|----------------------------------------------------|
| Simple string         | `binary()`                                         |
| Bulk string           | `binary()`                                         |
| Verbatim string       | `binary()`                                         |
| Array (multi-bulk)    | `list()`                                           |
| Map                   | `map()`                                            |
| Set                   | `sets:set()` (version 2 in OTP 24+)                |
| Null                  | `undefined`                                        |
| Boolean               | `boolean()`                                        |
| Integer               | `integer()`                                        |
| Big number            | `integer()`                                        |
| Float                 | `float()`, `inf`, `neg_inf`, `nan`                 |
| Error                 | `{error, binary()}`                                |
| Value with attributes | `{attribute, Value :: any(), Attributes :: map()}` |
| Push (out-of-band)    | `{push, list()}`                                   |

Pub/sub
-------

Ered supports pup/sub, including sharded pub/sub, when RESP3 is used. Pushed
messages are delivered to the push callback, so the connection option `push_cb`
needs to be provided. See [Connection options](#connection-options).

For the commands SUBSCRIBE, UNSUBSCRIBE, PSUBSCRIBE, PUNSUBSCRIBE, SSUBSCRIBE
and SUNSUBSCRIBE, `ered:command/3,4` returns `{ok, undefined}` on success and
one message for each channel, pattern or shard channel is delivered to the push
callback as a confirmation that subscribing or unsubscribing succeeded.

Subscriptions are tied to a connection. If the connection is lost, ered
reconnects automatically, but ered does not automatically subscribe to the same
channels again after a reconnect. If you want to subscribe again after
reconnect, [info messages](#info-messages) can be used to detect when a
connection goes down or comes up.
