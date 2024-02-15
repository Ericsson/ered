-module(ered_client_sup).

%% This is the supervisor for the ered_client processes of a custer client instance.

-export([start_link/0, start_client/4, stop_client/2]).

-behaviour(supervisor).
-export([init/1]).

-type host() :: inet:socket_address() | inet:hostname().
-type addr() :: {host(), inet:port_number()}.

start_link() ->
    supervisor:start_link(?MODULE, []).

-spec start_client(supervisor:sup_ref(), host(), inet:port_number(), [ered_client:opt()]) -> any().
start_client(Sup, Host, Port, ClientOpts) ->
    ChildSpec = #{id => {Host, Port},
                  start => {ered_client, start_link, [Host, Port, ClientOpts]},
                  modules => [ered_client]},
    supervisor:start_child(Sup, ChildSpec).

-spec stop_client(supervisor:sup_ref(), addr()) -> ok.
stop_client(Sup, Addr) ->
    ok = supervisor:terminate_child(Sup, Addr),
    ok = supervisor:delete_child(Sup, Addr).

init([]) ->
    %% Use defaults (one_for_one; tolerate 1 restart per 5 seconds), no children yet.
    {ok, {#{}, []}}.
