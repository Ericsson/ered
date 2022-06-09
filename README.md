ered
====

An Erlang client library for connecting to Redis Cluster
aiming to replace [eredis](https://github.com/Nordix/eredis) and [eredis_cluster](https://github.com/Nordix/eredis_cluster).

Status: WIP. See issues.

Features:

* status events
* queuing and load shedding (to handle burst traffic)
* pipelining of commands
* RESP3 support
* ASK redirection supporting hash tags
