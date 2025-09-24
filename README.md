ered
====

An Erlang client for connecting to Valkey clusters and standalone instances.

This projects is
aiming to replace [eredis](https://github.com/Nordix/eredis) and [eredis_cluster](https://github.com/Nordix/eredis_cluster).

It also works for open source versions of Redis, tested up to version 7.2.

Status: Beta. The API is not stable.

Features:

* status events
* queuing and load shedding (to handle burst traffic)
* automatic pipelining of commands - a single instance can be used simultaneously by multiple Erlang processes using only one connection to each database node
* [RESP3](https://valkey.io/topics/protocol/) support
* cluster and standalone mode

Usage by example
----------------

```Erlang
1> {ok, _} = application:ensure_all_started(ered, temporary),
2> {ok, Pid} = ered_cluster:connect([{"localhost", 6379}], []).
{ok,<0.164.0>}
3> ered_cluster:command(Pid, [<<"SET">>, <<"mykey">>, <<"42">>], <<"mykey">>, 5000).
{ok,<<"OK">>}
4> ered_cluster:command_async(Pid, [<<"GET">>, <<"mykey">>], <<"mykey">>, fun(Reply) -> io:format("Reply: ~p~n", [Reply]) end).
ok
Reply: {ok,<<"42">>}
5> ered:close(Pid).
ok
```

Overview
--------

There is an API for a single server (standalone mode) provided by the `ered`
module, and an API for cluster mode provided by the `ered_cluster` module.

The pid returned by the connect functions can be registered with a name and used
by multiple processes in parallel. The commands are multiplexed over the same
connections automatically.

A `command()` is a list of the command name and arguments as binaries. A batch
of pipelined commands can be provided as a list of lists of binaries and then
the returned reply value is a list of reply values.

A `reply()` is `{ok, Value} | {error, any()}` where `Value` is a nested
structure on the format described under [Valkey to Erlang Term
Representation](#valkey-to-erlang-term-representation).

For exact types, see the source code.

Standalone mode API
-------------------

The `ered` module is the client API to a single server instance.

### `ered:connect/3`

```Erlang
ered:connect(addr(), port(), [opt()]) -> {ok, pid()} | {error, term()}.
```

Connects to a single node. The process is supervised by the `ered`
application, which needs to be started in advance.

For options, see [Client options](#client-options) below.

### `ered:close/1`

```Erlang
ered:close(client_ref()) -> ok.
```

Closes the connection.

### `ered:command/2,3`

```Erlang
ered:command(client_ref(), command()) -> reply().
ered:command(client_ref(), command(), timeout()) -> reply().
```

Send a command and return the reply.
If the command is a single command then it is represented as a
list of binaries where the first binary is the command name
to execute and the rest of the binaries are the arguments.
If the command is a pipeline, e.g. multiple commands to executed
then they need to all map to the same slot for things to
work as expected.
For cluster clients, a key must be provided.
Omitting timeout is the same as setting the timeout to infinity.

### `ered:command_async/3`

```Erlang
ered:command_async(client_ref(), command(), fun((reply()) -> any())) -> ok.
```

Like command/2,3 but asynchronous. Instead of returning the reply, the reply
function is applied to the reply when it is available. The reply function runs
in an unspecified process and should not hang or perform any lengthy task.

Cluster mode API
----------------

The `ered_cluster` module is the API to a cluster.

### `ered_cluster:connect/2`

```Erlang
ered_cluster:connect([addr()], [ered_cluster:opt()]) -> {ok, pid()} | {error, term()}.
```

Connects to a cluster. This will start the cluster handling
process which will set up clients to the provided addresses and
fetch the cluster slot map. Once there is a complete slot map and
all clients processes are connected to their respective nodes, this
process is ready to serve requests. The processes are supervised by
the `ered` application, which needs to be started in advance.

One or more addresses, `addr() :: {inet:socket_address() | inet:hostname(),
inet:port_number()}`, are used to discover the rest of the cluster.

For options, see [Cluster options](#cluster-options) below.

### `ered_cluster:close/1`

```Erlang
ered_cluster:close(cluster_ref()) -> ok.
```

Stops the ered cluster handling process and closes all connections to the
cluster.

### `ered_cluster:command/3,4`

```Erlang
ered_cluster:command(cluster_ref(), command(), key()) -> reply().
ered_cluster:command(cluster_ref(), command(), key(), timeout()) -> reply().
```

Send a command. The command is routed to
the correct node based on the provided key.
If the command is a single command then it is represented as a
list of binaries where the first binary is the command name
to execute and the rest of the binaries are the arguments.
If the command is a pipeline, e.g. multiple commands to executed
then they need to all map to the same slot for things to
work as expected.
Omitting timeout is the same as setting the timeout to infinity.

### `ered_cluster:command_async/4`

```Erlang
ered_cluster:command_async(cluster_ref(), command(), key(), fun((reply()) -> any())) -> ok.
```

Like command/3,4 but asynchronous. Instead of returning the reply, the reply
function is applied to the reply when it is available. The reply function runs
in an unspecified process and should not hang or perform any lengthy task.

### `ered_cluster:command_all/2,3`

```Erlang
ered_cluster:command_all(cluster_ref(), command()) -> [reply()].
ered_cluster:command_all(cluster_ref(), command(), timeout()) -> [reply()].
```

Send the same command to all connected primary nodes.

### `ered_cluster:get_clients/1`

```Erlang
ered_cluster:get_clients(cluster_ref()) -> [client_ref()].
```

Get all primary node clients. Use the `ered` module for sending commands to the
individual instances.

### `ered_cluster:get_addr_to_client_map/1`

```Erlang
ered_cluster:get_addr_to_client_map(cluster_ref()) -> #{addr() => client_ref()}.
```

Get the address to client mapping. This includes all clients. Use the `ered`
module for sending commands to the individual instances.

### `ered_cluster:update_slots/1,2`

```Erlang
ered_cluster:update_slots(cluster_ref()) -> ok.
ered_cluster:update_slots(cluster_ref(), client_ref()) -> ok.
```

Manually trigger a slot mapping update. If a client pid or name is provided and
available, this client is used to fetch the slot map. Otherwise a random node is
used.

Options
-------

### Cluster options

The following options can be passed to `ered_cluster:connect/2`:

* `{try_again_delay, non_neg_integer()}`

  If there is a TRYAGAIN response from Valkey then wait this many milliseconds
  before re-sending the command. Default 200.

* `{redirect_attempts, non_neg_integer()}`

  Only do these many retries or re-sends before giving up and returning the
  result. This affects ASK, MOVED and TRYAGAIN responses. Default 10.

* `{info_pid, [pid()]}`

  List of pids to receive cluster info messages. See [Info
  messages](#info-messages) below.

* `{update_slot_wait, non_neg_integer()}`

  CLUSTER SLOTS command is used to fetch slots from the cluster. This
  value sets how long in milliseconds to wait before trying to send the command
  again. Default 500.

* `{client_opts, [ered_client:opt()]}`

  Options passed to each client. See [Client options](#client-options) below.

* `{min_replicas, non_neg_integer()}`

  For each primary node, the min number of replicas for the cluster
  to be considered OK. Default 0.

* `{convergence_check_timeout, timeout()}`

  If non-zero, a check that all primary nodes converge and report identical slot
  maps, is performed before the cluster is considered OK and the 'cluster_ok'
  info message is sent. The timeout is how long to wait for replies from all the
  primary nodes. Default 1000. Set to zero to disable this check.

* `{convergence_check_delay, timeout()}`

  If non-zero, a check that all primary nodes converge and report identical slot
  maps, is performed after a slot map update when the cluster is already
  considered OK, but only after the specified delay. Default 5000. Set to zero
  to disable this check. This option doesn't affect the convergence check
  performed when the cluster is not yet considered OK.

* `{close_wait, non_neg_integer()}`

  How long to delay the closing of clients that are no longer part of the slot
  map, in milliseconds. The delay is needed so that messages sent to the client
  are not lost in transit. Although the closing of these clients is delayed, the
  clients are put in a state so they respond immediately to commands with
  `{error, node_deactivated}`. Default 10000.

### Client options

Options passed to `ered:connect/3`. For `ered_cluster:connect/2`, the client
options are wrapped in `{client_opts, [...]}` and included in cluster options.

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

  What RESP (the serialization protocol) version to use. Default 3.

* `{node_down_timeout, non_neg_integer()}`

  If there is a connection problem, such as a timeout waiting for a response
  from Valkey, the ered client tries to set up a new connection. If the
  connection is not recovered within `node_down_timeout`, then the client
  considers the node down and clears it's queue (commands get an error reply)
  and starts rejecting all new commands until the connection is restored.
  Default 2000.

* `{use_cluster_id, boolean()}`

  Set to true if the CLUSTER ID should be fetched and used in info messages.
  This is set to true automatically when a client is started as part of a
  cluster client and false otherwise. (Not useful if the client is used outside
  of a cluster.)

* `{auth, {Username :: binary(), Password :: binary()}}`

  Username and password for Valkey authentication. If a password is configured
  without a username, use `default` as the username.

### Connection options

Options passed to `connect/2` as the options `{client_opts, [{connection_opts, [...]}]}`.

* `{batch_size, non_neg_integer()}`

  If commands are queued up in the process message queue, this is the maximum
  number of messages that will be received and sent in one call. Default 16.

* `{connect_timeout, timeout()}`

  Timeout passed to `gen_tcp:connect/4` or `ssl:connect/4`. Default infinity.

* `{tcp_options, [gen_tcp:connect_option()]}`

  Options passed to `gen_tcp:connect/4`.

* `{tcp_connect_timeout, timeout()}`

  Timeout passed to `gen_tcp:connect/4`. Default infinity.
  Deprecated. Replaced by `{connect_timeout, timeout()}`.

* `{tls_options, [ssl:tls_client_option()]}`

  Options passed to `ssl:connect/4`. If this config parameter is present
  TLS will be used.

* `{tls_connect_timeout, timeout()}`

  Timeout passed to ssl:connect/4. Default infinity.
  Deprecated. Replaced by `{connect_timeout, timeout()}`.

* `{push_cb, push_cb()}`

  Callback for push notifications.

* `{response_timeout, non_neg_integer()}`

  Timeout when waiting for a response from Valkey in milliseconds. Default 10000.

  When a timeout happens, the connection is closed and the client attempts to
  set up a new connection. See the client option `node_down_timeout` above.

Info messages
-------------

### Cluster specific messages

When one or more pids have been provided as the option `{info_pid, [pid()]}` to
`ered_cluster:connect/2`, these are the messages ered sends. All messages are
maps with at least the key `msg_type`.

Messages about the cluster as a whole:

* `#{msg_type := cluster_ok}` is sent when the cluster is up and running and
  ered is connected to all nodes.

* `#{msg_type := cluster_not_ok, reason := Reason}` is sent when something is
  wrong, where Reason is one of the following:

  * `master_down` if one of the primary nodes (formerly called masters) is down or unreachable.

  * `master_queue_full` if one of the primary nodes (formerly called masters) is so busy that the maximum
    number of queued commands for the node is reached and any further command to
    that node would be rejected immediately.

  * `pending` if ered has not yet established connections to all nodes.

  * `not_all_slots_covered` if some cluster slot doesn't belong to any node in
    the cluster.

  * `too_few_replicas` if any of the primary nodes has fewer replicas than the
    minimum number as specified using the option `{min_replicas,
    non_neg_integer()}`. See options above.

* `#{msg_type := slot_map_updated, slot_map := list(), map_version :=
  non_neg_integer(), addr := addr()}` is sent when the cluster slot-to-node
  mapping has been updated.

* `#{msg_type := cluster_slots_error_response, response := any(), addr :=
  addr()}` is sent when there was an error updating the cluster slot-to-node
  mapping. The `response` is either an error or the atom `empty` if the CLUSTER
  SLOTS returned an empty list, which is treated like an error.

* `#{msg_type := cluster_stopped, reason := any()}` when the ered cluster
  instance is closing down.

### Messages about a single connection

Messages about the connection to a single node, created using `ered:connect/3`
or created internally as part of a cluster, are on the following form:

```Erlang
#{msg_type := MsgType,
  reason := Reason,
  master => boolean(), % Optional. Added by ered_cluster.
  addr := addr(),
  client_id := pid(),
  cluster_id => binary() % Optional. Added by ered_cluster.
 }
```

The field `msg_type` identifies what kind of event has happened and is described
below. Reason depends on `msg_type`. Master describes whether the node is a
primary (formerly called master) or a replica. Addr is a tuple `{Host, Port}` to the Valkey node. Client id
is the pid of the `ered` client responsible for the connection. Cluster id is
assigned by Valkey and is used to identify the node within the cluster.

The possible values of the `msg_type` field for connection events are as
follows:

* `connected` when the connection to a node has been established. Reason is
  `none`.

* `connect_error` when connecting to the node fails or the TLS handshake fails.
  Reason is as returned by `gen_tcp:connect/4` or `ssl:connect/4`.

* `init_error` when one of the initial commands sent to Valkey has failed, such
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

Valkey to Erlang Term Representation
-----------------------------------

| Valkey (RESP3)        | Erlang                                             |
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
