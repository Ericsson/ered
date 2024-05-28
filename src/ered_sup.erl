-module(ered_sup).

%% This is the top-level supervisor of the ered application. The tree has the
%% following structure. Triple lines indicate multiple children.
%%
%%                      ered_app
%%                         |
%%                      ered_sup
%%                      /       \
%%         ered_client_sup     ered_cluster_sup
%%               ///               \\\
%%       ered_client             ered_dyn_cluster_sup
%%                                 /         \
%%                        ered_client_sup   ered_cluster
%%                              ///         (significant)
%%                        ered_client

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callback
-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    SupFlags = #{strategy => one_for_one},
    ChildSpecs = [#{id      => ered_client_sup,
                    restart => permanent,
                    type    => supervisor,
                    start   => {ered_client_sup, start_link, [{local, ered_standalone_sup}]}},
                  #{id      => ered_cluster_sup,
                    restart => permanent,
                    type    => supervisor,
                    start   => {ered_cluster_sup, start_link, []}}],
    {ok, {SupFlags, ChildSpecs}}.
