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
