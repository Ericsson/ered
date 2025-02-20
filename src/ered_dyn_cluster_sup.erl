-module(ered_dyn_cluster_sup).

-behaviour(supervisor).

%% API
-export([start_link/0, start_client_sup/1, start_cluster_mgr/5]).

%% Supervisor callback
-export([init/1]).

%% API
start_link() ->
    supervisor:start_link(?MODULE, []).

start_client_sup(Sup) ->
    ChildSpec = #{id      => ered_client_sup,
                  restart => temporary,
                  type    => supervisor,
                  start   => {ered_client_sup, start_link, []},
                  modules => [ered_client_sup]},
    supervisor:start_child(Sup, ChildSpec).

start_cluster_mgr(Sup, Addrs, Opts, ClientSup, User) ->
    ChildSpec = #{id          => ered_cluster,
                  restart     => temporary,
                  type        => worker,
                  significant => true,
                  start       => {ered_cluster, start_link, [Addrs, Opts, ClientSup, User]},
                  modules     => [ered_cluster]},
    supervisor:start_child(Sup, ChildSpec).

%% Supervisor callback
init([]) ->
    SupFlags = #{strategy      => one_for_all,
                 intensity     => 0,
                 period        => 3600,
                 auto_shutdown => any_significant},
    {ok, {SupFlags, []}}.
