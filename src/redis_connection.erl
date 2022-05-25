-module(redis_connection).

%% Managing the socket, sending commands and receiving replies.
%% Batches messages from the process queue. One process handles
%% writing to the socket and one handles the reading and decoding.
%% After a command is sent in the sending process a message is sent to
%% the reading process informing it about how many replies to expect
%% and who expects the result. No reconnection handling, if there is
%% an error the processes will exit.

-export([connect/2,
         connect/3,
         connect_async/3,
         command/2, command/3,
         command_async/3]).

-export_type([opt/0,
             result/0,
             host/0]).


%%%===================================================================
%%% Definitions
%%%===================================================================
-record(recv_st, {socket :: gen_tcp:socket(),
                  push_cb :: push_cb(),
                  timeout :: non_neg_integer(), % miliseconds
                  waiting = [] :: [wait_info()],
                  waiting_since :: undefined | integer() % erlang:monotonic_time(millisecond)
                 }).

-type opt() ::
        %% If commands are queued up in the process message queue this is the max
        %% amount of messages that will be received and sent in one call
        {batch_size, non_neg_integer()} |
        %% Options passed to gen_tcp:connect
        {tcp_options, [gen_tcp:connect_option()]} |
        %% Callback for push notifications
        {push_cb, push_cb()} |
        %% Timeout when waiting for a response from Redis. milliseconds
        {response_timeout, non_neg_integer()}.

-type result() :: redis_parser:parse_result().
-type push_cb() :: fun((result()) -> any()).
-type wait_info() ::
        {N :: non_neg_integer() | single,
         pid(),
         Ref :: any(),
         Acc :: [result()]}. % Acc used to store partial pipeline results
-type host() :: inet:socket_address() | inet:hostname().
-type connect_result() :: {ok, connection_ref()} | {error, timeout | inet:posix()}.
-type connection_ref() :: pid().
%%%===================================================================
%%% API
%%%===================================================================

%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-spec connect(host(), inet:port_number()) -> connect_result().
-spec connect(host(), inet:port_number(), [opt()]) -> connect_result().
%%
%% Connect to Redis node. Start send and receive process.
%% When the connection is closed a socket_closed message will be sent.
%% to the calling process.
%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
connect(Host, Port) ->
    connect(Host, Port, []).

connect(Host, Port, Opts) ->
    Pid = connect_async(Host, Port, Opts),
    receive
        {connected, Pid} ->
            {ok, Pid};
        {connect_error, Pid, Reason} ->
            {error, Reason}
    end.

%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-spec connect_async(host(), inet:port_number(), [opt()]) -> connection_ref().
%%
%% Connect to Redis node. Start send and receive process.
%% The function will return before connect is completed and a connected or
%% connect_error message will be sent to the calling process.
%% When the connection is closed a socket_closed message will be sent.
%% to the calling process.
%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
connect_async(Addr, Port, Opts) ->
    [error({badarg, BadOpt}) || BadOpt <- proplists:get_keys(Opts) -- [batch_size, tcp_options, push_cb, response_timeout]],
    BatchSize = proplists:get_value(batch_size, Opts, 16),
    TcpOptions = [{active, false}, binary] ++ proplists:get_value(tcp_options, Opts, []),
    Timeout = proplists:get_value(response_timeout, Opts, 10000),
    PushCb = proplists:get_value(push_cb, Opts, fun(_) -> ok end),
    Master = self(),
    spawn_link(
      fun() ->
              SendPid = self(),
              case gen_tcp:connect(Addr, Port, TcpOptions) of
                  {ok, Socket} ->
                      Master ! {connected, SendPid},
                      Pid = spawn_link(fun() ->
                                               ExitReason = recv_loop(Socket, PushCb, Timeout),
                                               %% Inform sending process about exit
                                               SendPid ! ExitReason
                                       end),
                      ExitReason = send_loop(Socket, Pid, BatchSize),
                      Master ! {socket_closed, SendPid, ExitReason};
                  {error, Reason} ->
                      Master ! {connect_error, SendPid, Reason}
              end
      end).

%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-spec command(connection_ref(), redis_command:command()) -> result().
-spec command(connection_ref(), redis_command:command(), timeout()) -> result().
%%
%% Send a command to the connected Redis node. The argument can be a
%% single command as a list of binaries, a pipeline of command as a
%% list of commands or a formatted redis_command.
%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
command(Connection, Command) ->
    command(Connection, Command, 10000).

command(Connection, Command, Timeout) ->
    link(Connection),
    Ref = make_ref(),
    Connection ! {send, self(), Ref, redis_command:convert_to(Command)},
    receive {Ref, Value} ->
            unlink(Connection),
            Value
    after Timeout ->
            unlink(Connection),
            {error, timeout}
    end.
%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-spec command_async(connection_ref(), redis_command:command(), any()) -> ok.
%%
%% Send a command to the connected Redis node in asynchronous
%% fashion. The provided callback function will be called with the
%% reply. Note that the callback function will executing in the redis
%% client process and should not hang or perform any lengthy task.
%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
command_async(Connection, Data, Ref) ->
    Connection ! {send, self(), Ref, redis_command:convert_to(Data)},
    ok.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%%
%% Receive logic
%%

recv_loop(Socket, PushCB, Timeout) ->
    ParseInit = redis_parser:next(redis_parser:init()),
    State = #recv_st{socket = Socket, push_cb = PushCB, timeout = Timeout},
    try
        recv_loop({ParseInit, State})
    catch
        % handle done, parse error, recv error
        throw:Reason ->
            {recv_exit, Reason}
    end.

recv_loop({ParseResult, State}) ->
    Next = case ParseResult of
               {need_more, BytesNeeded, ParserState} ->
                   read_socket(BytesNeeded, ParserState, State);
               {done, Value, ParserState} ->
                   handle_result(Value, ParserState, State)
           end,
    recv_loop(Next).

read_socket(BytesNeeded, ParserState, State) ->
    State1 = update_waiting(0, State),
    WaitTime = get_timeout(State1),
    case gen_tcp:recv(State1#recv_st.socket, BytesNeeded, WaitTime) of
        {ok, Data} ->
            {redis_parser:continue(Data, ParserState), State1};
        {error, timeout} when State1#recv_st.waiting == [] ->
            %% no command pending, try again
            read_socket(BytesNeeded, ParserState, State1);
        {error, Reason} ->
            throw(Reason)
    end.

handle_result({push, Value = [Type|_]}, ParserState, State)
  when Type == <<"subscribe">>; Type == <<"psubscribe">>; Type == <<"unsubscribe">>; Type == <<"punsubscribe">> ->
    %% Pub/sub in resp3 is a bit quirky. The push is supposed to be out of bound data not connected to any request
    %% but for subscribe and unsubscribe requests the reply will come as a push. The reply for these commands
    %% need to be handled as a regular reply otherwise things get out of sync. This was tested on Redis 6.0.8.
    handle_result(Value, ParserState, State);
handle_result({push, Value}, ParserState, State) ->
    PushCB = State#recv_st.push_cb,
    PushCB(Value),
    {redis_parser:next(ParserState), State};
handle_result(Value, ParserState, State) ->
    {{N, Pid, Ref, Acc}, State1} = pop_waiting(State),
    %% Check how many replies expected
    case N of
        single ->
            Pid ! {Ref, Value},
            {redis_parser:next(ParserState), State1};
        1 ->
            %% Last one, send the reply
            Pid ! {Ref, lists:reverse([Value | Acc])},
            {redis_parser:next(ParserState), State1};
        _ ->
            %% More left, save the reply and keep going
            State2 = push_waiting({N-1, Pid, Ref, [Value | Acc]}, State1),
            {redis_parser:next(ParserState), State2}
    end.

get_timeout(State) ->
    case State#recv_st.waiting_since of
        undefined ->
            State#recv_st.timeout;
        Since ->
            case State#recv_st.timeout - (erlang:monotonic_time(millisecond) - Since) of
                T when T < 0 -> 0;
                T -> T
            end
    end.

pop_waiting(State) ->
    State1 = update_waiting(infinity, State),
    [WaitInfo | Rest] = State1#recv_st.waiting,
    {WaitInfo, State1#recv_st{waiting = Rest}}.


push_waiting(WaitInfo,State) ->
    State#recv_st{waiting = [WaitInfo | State#recv_st.waiting]}.

update_waiting(Timeout, State) when State#recv_st.waiting == [] ->
    case receive Msg -> Msg after Timeout -> timeout end of
        {requests, Req, Time} ->
            State#recv_st{waiting = Req, waiting_since = Time};
        timeout ->
            State#recv_st{waiting_since = undefined};
        close_down ->
            throw(done)
    end;
update_waiting(_Timeout, State) ->
    State.

%%
%% Send logic
%%

send_loop(Socket, RecvPid, BatchSize) ->
    case receive_data(BatchSize) of
        {recv_exit, Reason} ->
            {recv_exit, Reason};
        {data, {Refs, Data}} ->
            Time = erlang:monotonic_time(millisecond),
            case gen_tcp:send(Socket, Data) of
                ok ->
                    %% send to recv proc to fetch the response
                    RecvPid ! {requests, Refs, Time},
                    send_loop(Socket, RecvPid, BatchSize);
                {error, Reason} ->
                    %% Give recv_loop time to finish processing
                    %% This will shut down recv_loop if it is waiting on socket
                    gen_tcp:shutdown(Socket, read_write),
                    %% This will shut down recv_loop if it is waiting for a reference
                    RecvPid ! close_down,
                    %% Ok, recv done, time to die
                    receive {recv_exit, _Reason} -> ok end,
                    {send_exit, Reason}
            end
    end.

receive_data(N) ->
    receive_data(N, infinity, []).

receive_data(0, _Time, Acc) ->
    {data, lists:unzip(lists:reverse(Acc))};
receive_data(N, Time, Acc) ->
    receive
        Msg ->
            case Msg of
                {recv_exit, Reason} ->
                    {recv_exit, Reason};
                {send, Pid, Ref, Commands} ->
                    {Count, Data} = redis_command:get_count_and_data(Commands),
                    RefInfo = {Count, Pid, Ref, []},
                    Acc1 = [{RefInfo, Data} | Acc],
                    receive_data(N, 0, Acc1)
            end
    after Time ->
            receive_data(0, 0, Acc)
    end.
