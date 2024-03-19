-module(ered_client_sup).

%% This is the supervisor for the ered_client processes of a custer client instance.

-export([start_link/0, start_link/1, start_client/5, stop_client/2]).

-behaviour(supervisor).
-export([init/1]).

-type host() :: inet:socket_address() | inet:hostname().

start_link() ->
    %% Used for the clients owned by a cluster instance.
    supervisor:start_link(?MODULE, []).

start_link(Name) ->
    %% Used for standalone client connections.
    supervisor:start_link(Name, ?MODULE, []).

-spec start_client(supervisor:sup_ref(), host(), inet:port_number(), [ered_client:opt()], pid()) -> any().
start_client(Sup, Host, Port, ClientOpts, User) ->
    supervisor:start_child(Sup, [Host, Port, ClientOpts, User]).

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
