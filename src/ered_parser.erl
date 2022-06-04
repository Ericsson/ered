-module(ered_parser).

%% RESP3 (REdis Serialization Protocol) parser.

-export([init/0,
         next/1,
         continue/2]).

-export_type([parse_return/0,
              parse_result/0
             ]).

%%%===================================================================
%%% Definitions
%%%===================================================================

-record(parser_state, {data = <<>> :: binary(), % Data left
                       next = fun parse_initial/1 :: parse_function(), % Next parsing action
                       bytes_needed = 0 :: bytes_needed() % Bytes required to complete action, 0 is unspecified
                      }).

-type bytes_needed() :: non_neg_integer().

-type parse_function() :: fun((binary()) -> {done, parse_result()} | {cont, parse_function(), bytes_needed()}).

-type parse_result() :: binary() | {error, binary()} | integer() | undefined | [parse_result()] | inf | neg_inf |
                        float() | true | false | #{parse_result() => parse_result()} |
                        {attribute, parse_result(), parse_result()} | {push | parse_result()}.


-type parse_return() :: {done, parse_result(), #parser_state{}} | {need_more, bytes_needed(), #parser_state{}}.

%%%===================================================================
%%% API
%%%===================================================================

%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-spec init() -> #parser_state{}.
%%
%% Init empty parser continuation
%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
init() ->
    #parser_state{}.

%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-spec next(#parser_state{}) -> parse_return().
%%
%% Get next result or continuation.
%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
next(State) ->
    parse(State).

%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
-spec continue(binary(), #parser_state{}) -> parse_return().
%%
%% Feed more data to the parser. Get next result or continuation.
%% - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
continue(NewData, State) ->
    Data = State#parser_state.data,
    parse(State#parser_state{data = <<Data/binary, NewData/binary>>}).

%%%===================================================================
%%% Internal functions
%%%===================================================================

parse(State=#parser_state{data=Data, next=Fun, bytes_needed=Bytes}) ->
    case token(Data, Bytes) of
        {need_more, LackingBytes} ->
            {need_more, LackingBytes, State};
        {Token, Rest} ->
            case Fun(Token) of
                {done, Result} ->
                    {done, Result, #parser_state{data=Rest}};
                {cont, NextFun, NextBytes} ->
                    parse(#parser_state{data=Rest, next=NextFun, bytes_needed=NextBytes})
            end
    end.

token(Data, 0) ->
    case binary:split(Data, <<"\r\n">>) of
        [_] -> {need_more, 0}; % not enough data for a token
        [Token, Rest] -> {Token, Rest}
    end;
token(Data, Bytes) ->
    case Data of
        <<Token:(Bytes-2)/binary, _:2/binary, Rest/binary>> -> {Token, Rest}; % _:2 is the message separator, skip it
        _ -> {need_more, Bytes - size(Data)}
    end.

parse_initial(Token) ->
    case Token of
        %% RESP2
        <<"+", Rest/binary>> -> {done, Rest}; % Simple string
        <<"-", Rest/binary>> -> {done, {error, Rest}}; % error string
        <<":", Rest/binary>> -> {done, parse_integer(Rest)};
        <<"$-1">>            -> {done, undefined}; % null bulk string
        <<"$?">>             -> parse_stream_string(); % bulk/blob string
        <<"$", Rest/binary>> -> parse_blob_string(parse_size(Rest)); % bulk/blob string
        <<"*-1">>            -> {done, undefined}; % null parse_array
        <<"*?">>             -> aggregate_stream(parse_array([]));
        <<"*", Rest/binary>> -> aggregate_N(parse_size(Rest), parse_array([]));
        %% RESP3
        <<"_" >>             -> {done, undefined}; % Null
        <<",inf">>           -> {done, inf}; % float inifinity
        <<",-inf">>          -> {done, neg_inf}; % negative infinity
        <<",", Rest/binary>> -> {done, parse_float(Rest)};
        <<"#t">>             -> {done, true};
        <<"#f">>             -> {done, false};
        <<"!", Rest/binary>> -> parse_blob_error(parse_size(Rest));
        <<"=", Rest/binary>> -> parse_blob_string(parse_size(Rest)); % Verbatim string
        <<"(", Rest/binary>> -> {done, parse_integer(Rest)}; % big int
        <<"%?">>             -> aggregate_stream(parse_map(#{}, none));
        <<"%", Rest/binary>> -> aggregate_N(parse_size(Rest)*2, parse_map(#{}, none)); % *2: one for key on for val
        <<"~?">>             -> aggregate_stream(parse_set(#{}));
        <<"~", Rest/binary>> -> aggregate_N(parse_size(Rest), parse_set(#{}));
        <<"|", Rest/binary>> -> parse_attribute(parse_size(Rest));
        <<">", Rest/binary>> -> parse_push(parse_size(Rest));
        _  -> throw({parse_error, {invalid_data, Token}})
    end.

parse_blob_error(Bytes) ->
    {cont, fun(Data) -> {done, {error, Data}} end, Bytes + 2}. % blob error

parse_blob_string(Bytes) ->
    {cont, fun(Data) -> {done, Data} end, Bytes + 2}. % + 2 for /r/n

parse_size(Data) ->
    case parse_integer(Data) of
        Val when Val < 0 -> throw({parse_error, {invalid_size, Val}});
        Val -> Val
    end.

parse_integer(Data) ->
    try binary_to_integer(Data)
    catch
        error:badarg -> throw({parse_error, {not_integer, Data}})
    end.

parse_float(Data) ->
    try binary_to_float(Data)
    catch
        error:badarg ->
            % maybe its a float without decimal
            try float(binary_to_integer(Data))
            catch
                error:badarg -> throw({parse_error, {not_float, Data}})
            end
    end.

parse_attribute(N) ->
    with_result(aggregate_N(N*2, parse_map(#{}, none)),
                fun(Result) -> aggregate(fun(Val) -> {done, {attribute, Val, Result}} end) end).

parse_push(N) ->
    with_result(aggregate_N(N, parse_array([])),
                fun(Result) -> {done, {push, Result}} end).

aggregate(Fun) ->
    {cont, aggregate(Fun, fun parse_initial/1), 0}.

aggregate(Fun, Next) ->
    fun(Token) ->
            case Next(Token) of
                {cont, Next2, Bytes} -> {cont, aggregate(Fun, Next2), Bytes};
                {done, Val} -> Fun(Val)
            end
    end.

parse_set(Acc) ->
    fun(done) -> Acc;
       (Val) -> parse_set(Acc#{Val => true})
    end.

parse_map(Acc, Key) ->
    fun(Val) ->
            case {Val, Key} of
                {done, none} -> Acc;
                {done, Key}  -> throw({parse_error, {map_key_not_used, Key}});
                {Val, none}  -> parse_map(Acc, Val);
                {Val, Key}   -> parse_map(Acc#{Key => Val}, none)
            end
    end.

parse_array(Acc) ->
    fun(done) -> lists:reverse(Acc);
       (Val) -> parse_array([Val|Acc])
    end.

parse_stream_string() ->
    {cont, parse_stream_string(<<>>), 0}.
parse_stream_string(Acc) ->
    fun(<<";0">>)             -> {done, Acc};
       (<<";", Rest/binary>>) -> {cont, parse_stream_string(Acc), parse_size(Rest) + 2};
       (Bin)                  -> {cont, parse_stream_string(<<Acc/binary, Bin/binary>>), 0}
    end.

aggregate_N(0, Fun) ->
    {done, Fun(done)};
aggregate_N(N, Fun) ->
    {cont, aggregate_N(N, Fun, fun parse_initial/1), 0}.

aggregate_N(N, Fun, Next) ->
    fun(Token) ->
            case Next(Token) of
                {cont, Next2, Bytes} -> {cont, aggregate_N(N, Fun, Next2), Bytes};
                {done, Val} -> aggregate_N(N-1, Fun(Val))
            end
    end.

aggregate_stream(Fun) ->
    {cont, aggregate_stream(Fun, fun parse_initial/1), 0}.

aggregate_stream(Fun, Next) ->
    fun(<<".">>) ->
            {done, Fun(done)};
       (Token) ->
            case Next(Token) of
                {cont, Next2, Bytes} -> {cont, aggregate_stream(Fun, Next2), Bytes};
                {done, Val} -> aggregate_stream(Fun(Val))
            end
    end.

with_result(Result, DoneFun) ->
    case Result of
        {done, Val}         -> DoneFun(Val);
        {cont, Next, Bytes} -> {cont, fun(Token) -> with_result(Next(Token), DoneFun) end, Bytes}
    end.
