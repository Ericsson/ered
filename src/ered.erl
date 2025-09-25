-module(ered).

%% External API for using connecting and sending commands to a single server
%% node. For the cluster API, use the ered_cluster module.

%% API
-export([connect/3,
         close/1,
         command/2, command/3,
         command_async/3]).

-export_type([opt/0,
              addr/0,
              command/0,
              reply/0,
              reply_fun/0,
              client_ref/0]).

%%%===================================================================
%%% Definitions
%%%===================================================================

-type opt()         :: ered_client:opt().
-type addr()        :: {host(), inet:port_number()}.
-type host()        :: inet:socket_address() | inet:hostname().
-type command()     :: ered_command:command().
-type reply()       :: ered_client:reply() | {error, unmapped_slot | client_down}.
-type reply_fun()   :: ered_client:reply_fun().
-type client_ref()  :: gen_server:server_ref().

%%%===================================================================
%%% API
%%%===================================================================

%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-spec connect(host(), inet:port_number(), [opt()]) -> {ok, pid()} | {error, term()}.
%%
%% Open a single client connection to a node.
%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
connect(Host, Port, Opts) ->
    ered_client:connect(Host, Port, Opts).

%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-spec close(client_ref()) -> ok.
%%
%% Closes the connection to a single node.
%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
close(Pid) ->
    ered_client:close(Pid).

%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-spec command(client_ref(), command()) -> reply().
-spec command(client_ref(), command(), timeout()) -> reply().
%%
%% Send a command.
%% If the command is a single command then it is represented as a
%% list of binaries where the first binary is the command name
%% to execute and the rest of the binaries are the arguments.
%% If the command is a pipeline, e.g. multiple commands to executed
%% then, it's represented as a list of lists of binaries.
%% Omitting timeout is the same as setting the timeout to infinity.
%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
command(Pid, Command) ->
    ered_client:command(Pid, Command, infinity).
command(Pid, Command, Timeout) when is_integer(Timeout); Timeout =:= infinity ->
    ered_client:command(Pid, Command, Timeout).

%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-spec command_async(client_ref(), command(), fun((reply()) -> any())) -> ok.
%%
%% Like command/2,3 but asynchronous. Instead of returning the reply,
%% the reply function is applied to the reply when it is available.
%% The reply function runs in an unspecified process and should not
%% hang or perform any lengthy task.
%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
command_async(Pid, Command, ReplyFun) when is_function(ReplyFun, 1) ->
    ered_client:command_async(Pid, Command, ReplyFun).
