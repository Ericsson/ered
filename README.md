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

Options
-------

The following options can be passed to `start_link/2`:

* `{try_again_delay, non_neg_integer()}`

  If there is a TRYAGAIN response from Redis then wait
  this many milliseconds before re-sending the command.

* `{redirect_attempts, non_neg_integer()}`

  Only do these many retries or re-sends before giving
  up and returning the result. This affects ASK, MOVED
  and TRYAGAIN responses

* `{info_pid, [pid()]}`

  List of pids to receive cluster info messages. See `ered_info_msg` module.

* `{update_slot_wait, non_neg_integer()}`

  CLUSTER SLOTS command is used to fetch slots from the Redis cluster.
  This value sets how long to wait before trying to send the command again.

* `{client_opts, [ered_client:opt()]}`

  Options passed to each client. See [Client options](#client-options) below.

* `{min_replicas, non_neg_integer()}`

  For each Redis master node, the min number of replicas for the cluster
  to be considered OK.

* `{close_wait, non_neg_integer()}`

  How long to delay the closing of clients that are no longer part of
  the slot map. The delay is needed so that messages sent to the client
  are not lost in transit.

### Client options

Options passed to `start_link/2` as the options `{client_opts, [...]}`.

* `{connection_opts, [ered_connection:opt()]}`

  Options passed to the connection module. See [Connection options](#connection-options) below.

* `{max_waiting, non_neg_integer()}`

  Max number of commands allowed to wait in queue.

* `{max_pending, non_neg_integer()}`

  Max number of commands to be pending, i.e. sent to client
  and waiting for a response.

* `{queue_ok_level, non_neg_integer()}`

  If the queue has been full then it is considered ok
  again when it reaches this level

* `{reconnect_wait, non_neg_integer()}`

  How long to wait to reconnect after a failed connect attempt

* `{info_pid, none | pid()}`

  Pid to send status messages to

* `{resp_version, 2..3}`

  What RESP (REdis Serialization Protocol) version to use

* `{node_down_timeout, non_neg_integer()}`

  If there is a connection problem and the connection is
  not recovered before this timeout then the client considers
  the node down and will clear it's queue and reject all new
  commands until connection is restored.

* `{use_cluster_id, boolean()}`

  Set if the CLUSTER ID should be fetched used in info messages.
  (not useful if the client is used outside of a cluster)

### Connection options

Options passed to `start_link/2` as the options `{client_opts, [{connection_opts, [...]}]}`.

* `{batch_size, non_neg_integer()}`

  If commands are queued up in the process message queue this is the max
  amount of messages that will be received and sent in one call

* `{tcp_options, [gen_tcp:connect_option()]}`

  Options passed to gen_tcp:connect

* `{tls_options, [ssl:tls_client_option()]}`

  Options passed to ssl:connect. If this config parameter is present
  tls will be used instead of tcp

* `{push_cb, push_cb()}`

  Callback for push notifications

* `{response_timeout, non_neg_integer()}`

  Timeout when waiting for a response from Redis, in milliseconds

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
