-module(ered_client_sup).

%% This is the supervisor for the ered_client processes of a custer client instance.

-export([start_link/0, start_client/4, stop_client/2]).

-behaviour(supervisor).
-export([init/1]).

-type host() :: inet:socket_address() | inet:hostname().

start_link() ->
    supervisor:start_link(?MODULE, []).

-spec start_client(supervisor:sup_ref(), host(), inet:port_number(), [ered_client:opt()]) -> any().
start_client(Sup, Host, Port, ClientOpts) ->
    supervisor:start_child(Sup, [Host, Port, ClientOpts]).

-spec stop_client(supervisor:sup_ref(), pid()) -> ok.
stop_client(Sup, Pid) ->
    _ = supervisor:terminate_child(Sup, Pid),
    ok.

init([]) ->
    {ok, {#{strategy => simple_one_for_one},
          [#{id => undefined,
             start => {ered_client, start_link, []},
             restart => temporary,
             modules => [ered_client]}]}}.
