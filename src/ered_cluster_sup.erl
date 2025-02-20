-module(ered_cluster_sup).

-behaviour(supervisor).

%% API
-export([start_link/0, start_child/0]).

%% Supervisor callback
-export([init/1]).

%% API
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

start_child() ->
    supervisor:start_child(?MODULE, []).

init([]) ->
    SupFlags = #{strategy => simple_one_for_one},
    ChildSpecs = [#{id      => undefined,
                    restart => temporary,
                    type    => supervisor,
                    start   => {ered_dyn_cluster_sup, start_link, []}}],
    {ok, {SupFlags, ChildSpecs}}.
