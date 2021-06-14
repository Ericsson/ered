-module(redis_connection).

-export([connect/2, connect/3, connect_async/3, request/2, request/3, request_async/3, request_async_raw/3]).



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
            timeout
    end.

request_async(Connection, Data, Ref) ->
    Connection ! {send, self(), Ref, redis_lib:format_request(Data)},
    ok.

request_async_raw(Connection, Data, Ref) ->
    Connection ! {send, self(), Ref, Data},
    ok.

connect(Host, Port) ->
    connect(Host, Port, []).

connect(Host, Port, Opts) ->
    Pid = connect_async(Host, Port, Opts),
    receive {connected, Pid} -> Pid end.

connect_async(Addr, Port, Opts) ->
    [error({badarg, BadOpt}) || BadOpt <- proplists:get_keys(Opts) -- [batch_size, tcp_options, push_cb, response_timeout]],
    BatchSize = proplists:get_value(batch_size, Opts, 16),
    TcpOptions = [{active, false}, binary] ++ proplists:get_value(tcp_options, Opts, []),
    Timeout = proplists:get_value(response_timeout, Opts, 10000),
    PushCb = proplists:get_value(push_cb, Opts, fun(_) -> ok end),
    Master = self(),
    spawn_link(
      fun() ->
              case gen_tcp:connect(Addr, Port, TcpOptions) of
                  {ok, Socket} ->
                      Master ! {connected, self()},
                      Pid = spawn_link(fun() -> recv_loop(Socket, PushCb, Timeout) end),
                      send_loop(Socket, Pid, BatchSize);
                  {error, Reason} ->
                      exit({connect_error, Reason})
              end
      end).


recv_loop(Socket, PushCB, Timeout) ->
    fun Loop({ParseResult, Requests}) ->
            Loop(try ParseResult of
                     {need_more, Bytes, ParserState} -> read_socket(Bytes, ParserState, Requests, Socket, Timeout);
                     {done, Value, ParserState} -> handle_result(Value, ParserState, Requests, PushCB)
                 catch % handle done, parse error, recv error
                     throw:Error -> exit(Error)
                 end)
    end({redis_parser:next(redis_parser:init()), {_Requests=[], _Time=undefined}}).

read_socket(Bytes, ParserState, Requests, Socket, Timeout) ->
    {_, ReqTime} = UpdatedRequests = update_requests(nonblock, Requests),
    case gen_tcp:recv(Socket, Bytes, get_timeout(ReqTime, Timeout)) of
        {ok, Data} ->
            {redis_parser:continue(Data, ParserState), UpdatedRequests};
        {error, timeout} when ReqTime == undefined ->
            %% no requests pending, try again
            read_socket(Bytes, ParserState, UpdatedRequests, Socket, Timeout);
        {error, Reason} ->
            throw({recv_error, Reason})
    end.

handle_result({push, Value = [Type|_]}, ParserState, Requests, PushCB)
  when Type == <<"subscribe">>; Type == <<"psubscribe">>; Type == <<"unsubscribe">>; Type == <<"punsubscribe">> ->
    %% Pub/sub in resp3 is a bit quirky. The push is supposed to be out of bound data not connected to any request
    %% but for subscribe and unsubscribe requests the reply will come as a push. The reply for these commands
    %% need to be handled as a regular reply otherwise things get out of sync. This was tested on Redis 6.0.8.
    handle_result(Value, ParserState, Requests, PushCB);
handle_result({push, Value}, ParserState, Requests, PushCB) ->
    PushCB(Value),
    {redis_parser:next(ParserState), Requests};
handle_result(Value, ParserState, Requests, _PushCB) ->
    {[{Pid, Ref} | Reqs], ReqTime} = update_requests(block, Requests),
    Pid ! {Ref, Value},
    {redis_parser:next(ParserState), {Reqs, ReqTime}}.


update_requests(Mode, {[], _}) ->
    Timeout = case Mode of block -> infinity; nonblock -> 0 end,
    case receive Msg -> Msg after Timeout -> timeout end of
        {requests, Req, Time} -> {Req, Time};
        timeout -> {[], undefined};
        close_down -> throw(done)
    end;
update_requests(_, {Req, Time}) ->
    {Req, Time}.

get_timeout(undefined, Timeout) ->
    Timeout;
get_timeout(SendTime, Timeout) ->
    case Timeout - (erlang:monotonic_time(millisecond) - SendTime) of
        T when T < 0 -> 0;
        T -> T
    end.

send_loop(Socket, RecvPid, BatchSize) ->
    {Refs, Datas} = lists:unzip([{{Pid, Ref}, Data} || {send, Pid, Ref, {redis_command, Data}} <- receive_multiple(BatchSize)]),
    Time = erlang:monotonic_time(millisecond),
    case gen_tcp:send(Socket, Datas) of
        ok ->
            %% send to recv proc to fetch the response
            RecvPid ! {requests, Refs, Time};
        {error, Reason} ->
            % Give recv_loop time to finish processing
            process_flag(trap_exit, true),
            % This will shut down recv_loop if it is waiting on socket
            gen_tcp:shutdown(Socket, read_write),
            % This will shut down recv_loop if it is waiting for a reference
            RecvPid ! close_down,
            % Ok, recv done, time to die
            receive {'EXIT', _, _} -> exit({send_error, Reason}) end
    end,
    send_loop(Socket, RecvPid, BatchSize).


receive_multiple(N) -> [receive Msg -> Msg end | receive_multiple_rest(N-1)].

receive_multiple_rest(0) ->
    [];
receive_multiple_rest(N) ->
    receive Msg -> [Msg | receive_multiple_rest(N-1)]
    after 0 -> []
    end.

