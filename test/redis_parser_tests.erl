-module(redis_parser_tests).

-include_lib("eunit/include/eunit.hrl").

test_data() ->
    [
     %% Test data taken from the Redis Protocol (RESP) specification
     %% https://redis.io/topics/protocol
     {"simple_string",     <<"+OK\r\n">>,                                              <<"OK">>} ,
     {"error msg",         <<"-Error message\r\n">>,                                   {error, <<"Error message">>}},
     {"integer",           <<":1000\r\n">>,                                            1000},
     {"bulk string",       <<"$6\r\nfoobar\r\n">>,                                     <<"foobar">>},
     {"empty bulk string", <<"$0\r\n\r\n">>,                                           <<"">>},
     {"null bulk string",  <<"$-1\r\n">>,                                              undefined},
     {"empty array",       <<"*0\r\n">>,                                               []},
     {"string array",      <<"*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n">>,                     [<<"foo">>, <<"bar">>]},
     {"integer array",     <<"*3\r\n:1\r\n:2\r\n:3\r\n">>,                             [1,2,3]},
     {"mixed array",       <<"*5\r\n:1\r\n:2\r\n:3\r\n:4\r\n$6\r\nfoobar\r\n">>,       [1,2,3,4,<<"foobar">>]},
     {"null array",        <<"*-1\r\n">>,                                              undefined},
     {"nested array",      <<"*2\r\n*3\r\n:1\r\n:2\r\n:3\r\n*2\r\n+Foo\r\n-Bar\r\n">>, [[1,2,3], [<<"Foo">>, {error, <<"Bar">>}]]},
     {"null in array",     <<"*3\r\n$3\r\nfoo\r\n$-1\r\n$3\r\nbar\r\n">>,              [<<"foo">>, undefined, <<"bar">>]},

     %% Test data taken from the RESP3 specification
     %% https://github.com/antirez/RESP3/blob/master/spec.md
     {"null",              <<"_\r\n">>,                                                undefined},
     {"float",             <<",1.23\r\n">>,                                            1.23},
     {"float no decimal",  <<",10\r\n">>,                                              10.0},
     {"float inf",         <<",inf\r\n">>,                                             inf},
     {"float neg inf",     <<",-inf\r\n">>,                                            neg_inf},
     {"boolean true",      <<"#t\r\n">>,                                               true},
     {"boolean false",     <<"#f\r\n">>,                                               false},
     {"blob error",        <<"!21\r\nSYNTAX invalid syntax\r\n">>,                     {error, <<"SYNTAX invalid syntax">>}},
     {"verbatim string",   <<"=15\r\ntxt:Some string\r\n">>,                           <<"txt:Some string">>},
     {"big number",        <<"(3492890328409238509324850943850943825024385\r\n">>,     3492890328409238509324850943850943825024385},
     {"map",               <<"%2\r\n+first\r\n:1\r\n+second\r\n:2\r\n">>,              #{<<"first">> => 1, <<"second">> => 2}},
     {"set",               <<"~5\r\n+orange\r\n+apple\r\n#t\r\n:100\r\n:999\r\n">>,    #{<<"orange">> => true, <<"apple">> => true,
                                                                                        true => true, 100 => true, 999 => true}},
     {"attribute",         <<"|1\r\n+key-popularity\r\n%2\r\n$1\r\na\r\n,0.1923\r\n"
                             "$1\r\nb\r\n,0.0012\r\n*2\r\n:2039123\r\n:9543892\r\n">>,  {attribute, [2039123, 9543892],
                                                                                         #{<<"key-popularity">> =>
                                                                                               #{<<"a">> => 0.1923,
                                                                                                 <<"b">> => 0.0012}}}},
     {"nested attribute",  <<"*3\r\n:1\r\n:2\r\n|1\r\n+ttl\r\n:3600\r\n:3\r\n">>,       [1, 2, {attribute, 3, #{<<"ttl">> =>3600}}]},
     {"push",              <<">4\r\n+pubsub\r\n+message\r\n+somechannel\r\n"
                             "+this is the message\r\n">>,                              {push, [<<"pubsub">>, <<"message">>,
                                                                                                <<"somechannel">>,
                                                                                                <<"this is the message">>]}},
     {"streamed string",  <<"$?\r\n;4\r\nHell\r\n;5\r\no wor\r\n;1\r\nd\r\n;0\r\n">>,   <<"Hello word">>},
     {"streamed array",   <<"*?\r\n:1\r\n:2\r\n:3\r\n.\r\n">>,                          [1, 2, 3]},
     {"streamed map",     <<"%?\r\n+a\r\n:1\r\n+b\r\n:2\r\n.\r\n">>,                    #{<<"a">> => 1, <<"b">> => 2}},
      %% Additional tests
     {"streamed set",     <<"~?\r\n+a\r\n:1\r\n+b\r\n:2\r\n.\r\n">>,                    #{<<"a">> => true, 1 => true,
                                                                                          <<"b">> => true, 2 => true}},
     {"float negative", <<",-1.23\r\n">>, -1.23}
    ].

normal_test_() ->
    run_with(fun(In, Out) -> run([In], Out) end).

split_input_test_() ->
    % split the input into a list of binaries, the result should be the same
    run_with(fun(In, Out) -> run([<<C>> || C <- binary:bin_to_list(In)], Out) end).

parse_fail_test_() ->
    [
     {"invalid type",       decode_err(<<"Apa\r\n">>,           {invalid_data, <<"Apa">>})},
     {"not int",            decode_err(<<":Apa\r\n">>,          {not_integer, <<"Apa">>})},
     {"not float",          decode_err(<<",Apa\r\n">>,          {not_float, <<"Apa">>})},
     {"not bool",           decode_err(<<"#Apa\r\n">>,          {invalid_data, <<"#Apa">>})},
     {"invalid array size", decode_err(<<"*-2\r\n">>,           {invalid_size, -2})},
     {"invalid map size",   decode_err(<<"%-1\r\n">>,           {invalid_size, -1})},
     {"invalid set size",   decode_err(<<"~-1\r\n">>,           {invalid_size, -1})},
     {"uneaven map",        decode_err(<<"%?\r\n+a\r\n.\r\n">>, {map_key_not_used, <<"a">>})}
    ].

decode_err(In, Expected) ->
    fun() ->
            try
                A = redis_parser:continue(In, redis_parser:init()),
                exit({unexpected_success, A})
            catch
                throw:{parse_error, Err} ->
                    ?assertEqual(Expected, Err)
            end
    end.


run_with(Fun) ->
    lists:map(fun({Desc, In, Out}) -> {Desc, Fun(In, Out)} end, test_data()).

run(DataList, Expected) when is_list(DataList) ->
    fun() ->
            fun F([Data|Rest], State) ->
                    case redis_parser:continue(Data, State) of
                        {done, Result, NewState} ->
                            ?assertEqual(Expected, Result),
                            % No lefteover data in state
                            ?assertEqual(redis_parser:init(), NewState),
                            % No unparsed data
                            ?assertEqual([], Rest);
                        {need_more, _, NewState} ->
                            F(Rest, NewState)
                    end
            end(DataList, redis_parser:init())
    end.
