-module(redis_connection).

-export([connect/2, connect/3, connect_async/3, request/2, request/3, request_async/3, request_async_raw/3]).


%% -type option() ::
%%         {batch_size, non_neg_integer()} |
%%         {tcp_options, [inet:inet_backend() | connect_option()]} |
%%         {push_cb, fun(
%%          {response_timeout, non_neg_integer()}

% record(recvst {socket, refs = [], push_cb}).
% f(P), P = redis:connect("192.168.1.5", 6379), redis:request(P, <<"ping\r\n">>).

%% TODO
%%
%% send quit at close

request(Connection, Data) ->
    request(Connection, Data, 10000).

request(Connection, Data, Timeout) ->
    link(Connection),
    Ref = make_ref(),
    Connection ! {send, self(), Ref, redis_lib:format_request(Data)},
    receive {Ref, Value} ->
            unlink(Connection),
            Value
    after Timeout ->
            unlink(Connection),
            {error, timeout}
    end.

request_async(Connection, Data, Ref) ->
    Connection ! {send, self(), Ref, redis_lib:format_request(Data)},
    ok.

request_async_raw(Connection, Data, Ref) ->
    Connection ! {send, self(), Ref, {redis_command, 1, Data}},
    ok.

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

%% ++++++++++++++++++++++++++++++++++++++
%% Receive logic
%% ++++++++++++++++++++++++++++++++++++++
-record(recv_st, {socket,
                  push_cb,
                  timeout,
                  waiting = [],
                  waiting_since}).


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

%% recv_loop({ParseResult, State}) ->
%%     Next = try
%%                case ParseResult of
%%                    {need_more, BytesNeeded, ParserState} ->
%%                        read_socket(BytesNeeded, ParserState, State);
%%                    {done, Value, ParserState} ->
%%                        handle_result(Value, ParserState, State)
%%                end
%%            catch % handle done, parse error, recv error
%%                throw:Error ->
%%                    State#recv_state.master_pid ! Error
%%            end,
%%     recv_loop(Next).

read_socket(BytesNeeded, ParserState, State) ->
    State1 = update_waiting(0, State),
    WaitTime = get_timeout(State1),
    case gen_tcp:recv(State1#recv_st.socket, BytesNeeded, WaitTime) of
        {ok, Data} ->
            {redis_parser:continue(Data, ParserState), State1};
        {error, timeout} when State1#recv_st.waiting == [] ->
            %% no requests pending, try again
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

%% ++++++++++++++++++++++++++++++++++++++
%% Send logic
%% ++++++++++++++++++++++++++++++++++++++
%% send_loop(Socket, RecvPid, BatchSize) ->
%%     {Refs, Datas} = lists:unzip([{{N, Pid, Ref, []}, Data} ||
%%                                     {send, Pid, Ref, {redis_command, N, Data}} <- receive_multiple(BatchSize)]),
%%     Time = erlang:monotonic_time(millisecond),
%%     case gen_tcp:send(Socket, Datas) of
%%         ok ->
%%             %% send to recv proc to fetch the response
%%             RecvPid ! {requests, Refs, Time},
%%             send_loop(Socket, RecvPid, BatchSize);
%%         {error, Reason} ->
%%             % Give recv_loop time to finish processing
%%             process_flag(trap_exit, true),
%%             % This will shut down recv_loop if it is waiting on socket
%%             gen_tcp:shutdown(Socket, read_write),
%%             % This will shut down recv_loop if it is waiting for a reference
%%             RecvPid ! close_down,
%%             % Ok, recv done, time to die
%%             receive {'EXIT', _, _} -> ok end,
%%             Reason
%%     end.




%% receive_messages() ->
%%     receive_multiple(N)

%% collect_data([], Acc) ->
%%     {data, Acc};
%% collect_data([{recv_exit, Reason}|_], _Acc) ->
%%     {recv_exit, Reason};
%% collect_data([{send, Pid, Ref, {redis_command, N, Data} | Msgs], {Refs, Data}}) ->
%%     RefInfo = {N, Pid, Ref, []},
%%     collect_data(Msgs, {RefIn

%% receive_multiple(N) ->
%%     %% Always get atleast one message
%%     [receive Msg -> Msg end | receive_multiple_rest(N-1)].

%% receive_multiple_rest(0) ->
%%     [];
%% receive_multiple_rest(N) ->
%%     receive Msg ->
%%             [Msg | receive_multiple_rest(N-1)]
%%     after 0 ->
%%             []
%%     end.


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
                {send, Pid, Ref, {redis_command, Count, Data}} ->
                    RefInfo = {Count, Pid, Ref, []},
                    Acc1 = [{RefInfo, Data} | Acc],
                    receive_data(N, 0, Acc1)
            end
    after Time ->
            receive_data(0, 0, Acc)
    end.


%%     %% Always get atleast one message
%%     [receive Msg -> Msg end | receive_multiple_rest(N-1)].

%% receive_multiple_rest(0) ->
%%     [];
%% receive_multiple_rest(N) ->
%%     receive Msg ->
%%             [Msg | receive_multiple_rest(N-1)]
%%     after Time ->
%%             []
%%     end.



