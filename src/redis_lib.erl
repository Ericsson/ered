-module(redis_lib).

%% TODO: Split this module into several?

-export([format_command/1,
         slotmap_master_slots/1,
         slotmap_master_nodes/1,
         slotmap_all_nodes/1,
         hash/1]).


-type request() :: {redis_command,
                    non_neg_integer() | single,
                    binary() | [binary()]}.

-export_type([request/0]).

-spec format_command(binary() | iolist() | request()) -> request().

format_command(Command = {redis_command, _, _}) ->
    Command;

format_command(Data) when is_binary(Data) ->
    format_command([Data]);

format_command([]) ->
    error({badarg, []});

format_command(RawCommands = [E|_]) when is_list(E) ->
    Commands = [command_to_bin(RawCommand) || RawCommand <- RawCommands],
    {redis_command, length(RawCommands), Commands};

format_command(RawCommand) ->
    Command = command_to_bin(RawCommand),
    {redis_command, single, Command}.

command_to_bin(RawCommand) ->
    Len = integer_to_list(length(RawCommand)),
    Elements = [["$", integer_to_list(size(Bin)), "\r\n", Bin, "\r\n"] || Bin <- RawCommand],
    %% Maybe this could be kept as an iolist?
    %% TODO profile this.
    %% Since this is copied around a bit between processes it might be cheaper to keep it as a binary
    %% since then it will be heap allocated if big. Just pure speculation..
    iolist_to_binary(["*", Len, "\r\n", Elements]).


slotmap_master_slots(ClusterSlotsReply) ->

    %% [[10923,16383,
    %%   [<<"127.0.0.1">>,30003,
    %%    <<"3d87c864459cb190be1a272e6096435e87721c94">>],
    %%   [<<"127.0.0.1">>,30006,
    %%    <<"12d0ac6c30fcbec08555831bf81afe8d5c0c1d4b">>]],
    %%  [0,5460,
    %%   [<<"127.0.0.1">>,30001,
    %%    <<"1127e053184e563727ee7d10f1f4851127f6f064">>],
    %%   [<<"127.0.0.1">>,30004,
    %%    <<"2dc6838de2543a104b623bd986013e24e7260eb6">>]],
    %%  [5461,10922,
    %%   [<<"127.0.0.1">>,30002,
    %%    <<"848879a1027f7a95ea058f3ca13a08bf4a70d7db">>],
    %%   [<<"127.0.0.1">>,30005,
    %%    <<"6ef9ab5fc9b66b63b469c5f53978a237e65d42ce">>]]]

    %% TODO: Maybe wrap this in a try catch if we get garbage?
    SlotMap = [{SlotStart, SlotEnd, node_info(Master)}
               || [SlotStart, SlotEnd, Master | _] <- ClusterSlotsReply],
    lists:sort(SlotMap).

slotmap_master_nodes(ClusterSlotsReply) ->
    Nodes = [node_info(Master) || [_SlotStart, _SlotEnd, Master | _] <- ClusterSlotsReply],
    lists:sort(Nodes).


slotmap_all_nodes(ClusterSlotsReply) ->
    AllNodes = [lists:map(fun node_info/1, Nodes) || [_SlotStart, _SlotEnd | Nodes] <- ClusterSlotsReply],
    lists:sort(lists:append(AllNodes)).

node_info([Ip, Port |_]) ->
    {binary_to_list(Ip), Port}.

hash(Key) ->
    crc16(0, Key) rem 16384.

-define(CRCTAB, <<16#00,16#00,16#10,16#21,16#20,16#42,16#30,16#63,16#40,16#84,16#50,16#a5,16#60,16#c6,16#70,16#e7,
		  16#81,16#08,16#91,16#29,16#a1,16#4a,16#b1,16#6b,16#c1,16#8c,16#d1,16#ad,16#e1,16#ce,16#f1,16#ef,
		  16#12,16#31,16#02,16#10,16#32,16#73,16#22,16#52,16#52,16#b5,16#42,16#94,16#72,16#f7,16#62,16#d6,
		  16#93,16#39,16#83,16#18,16#b3,16#7b,16#a3,16#5a,16#d3,16#bd,16#c3,16#9c,16#f3,16#ff,16#e3,16#de,
		  16#24,16#62,16#34,16#43,16#04,16#20,16#14,16#01,16#64,16#e6,16#74,16#c7,16#44,16#a4,16#54,16#85,
		  16#a5,16#6a,16#b5,16#4b,16#85,16#28,16#95,16#09,16#e5,16#ee,16#f5,16#cf,16#c5,16#ac,16#d5,16#8d,
		  16#36,16#53,16#26,16#72,16#16,16#11,16#06,16#30,16#76,16#d7,16#66,16#f6,16#56,16#95,16#46,16#b4,
		  16#b7,16#5b,16#a7,16#7a,16#97,16#19,16#87,16#38,16#f7,16#df,16#e7,16#fe,16#d7,16#9d,16#c7,16#bc,
		  16#48,16#c4,16#58,16#e5,16#68,16#86,16#78,16#a7,16#08,16#40,16#18,16#61,16#28,16#02,16#38,16#23,
		  16#c9,16#cc,16#d9,16#ed,16#e9,16#8e,16#f9,16#af,16#89,16#48,16#99,16#69,16#a9,16#0a,16#b9,16#2b,
		  16#5a,16#f5,16#4a,16#d4,16#7a,16#b7,16#6a,16#96,16#1a,16#71,16#0a,16#50,16#3a,16#33,16#2a,16#12,
		  16#db,16#fd,16#cb,16#dc,16#fb,16#bf,16#eb,16#9e,16#9b,16#79,16#8b,16#58,16#bb,16#3b,16#ab,16#1a,
		  16#6c,16#a6,16#7c,16#87,16#4c,16#e4,16#5c,16#c5,16#2c,16#22,16#3c,16#03,16#0c,16#60,16#1c,16#41,
		  16#ed,16#ae,16#fd,16#8f,16#cd,16#ec,16#dd,16#cd,16#ad,16#2a,16#bd,16#0b,16#8d,16#68,16#9d,16#49,
		  16#7e,16#97,16#6e,16#b6,16#5e,16#d5,16#4e,16#f4,16#3e,16#13,16#2e,16#32,16#1e,16#51,16#0e,16#70,
		  16#ff,16#9f,16#ef,16#be,16#df,16#dd,16#cf,16#fc,16#bf,16#1b,16#af,16#3a,16#9f,16#59,16#8f,16#78,
		  16#91,16#88,16#81,16#a9,16#b1,16#ca,16#a1,16#eb,16#d1,16#0c,16#c1,16#2d,16#f1,16#4e,16#e1,16#6f,
		  16#10,16#80,16#00,16#a1,16#30,16#c2,16#20,16#e3,16#50,16#04,16#40,16#25,16#70,16#46,16#60,16#67,
		  16#83,16#b9,16#93,16#98,16#a3,16#fb,16#b3,16#da,16#c3,16#3d,16#d3,16#1c,16#e3,16#7f,16#f3,16#5e,
		  16#02,16#b1,16#12,16#90,16#22,16#f3,16#32,16#d2,16#42,16#35,16#52,16#14,16#62,16#77,16#72,16#56,
		  16#b5,16#ea,16#a5,16#cb,16#95,16#a8,16#85,16#89,16#f5,16#6e,16#e5,16#4f,16#d5,16#2c,16#c5,16#0d,
		  16#34,16#e2,16#24,16#c3,16#14,16#a0,16#04,16#81,16#74,16#66,16#64,16#47,16#54,16#24,16#44,16#05,
		  16#a7,16#db,16#b7,16#fa,16#87,16#99,16#97,16#b8,16#e7,16#5f,16#f7,16#7e,16#c7,16#1d,16#d7,16#3c,
		  16#26,16#d3,16#36,16#f2,16#06,16#91,16#16,16#b0,16#66,16#57,16#76,16#76,16#46,16#15,16#56,16#34,
		  16#d9,16#4c,16#c9,16#6d,16#f9,16#0e,16#e9,16#2f,16#99,16#c8,16#89,16#e9,16#b9,16#8a,16#a9,16#ab,
		  16#58,16#44,16#48,16#65,16#78,16#06,16#68,16#27,16#18,16#c0,16#08,16#e1,16#38,16#82,16#28,16#a3,
		  16#cb,16#7d,16#db,16#5c,16#eb,16#3f,16#fb,16#1e,16#8b,16#f9,16#9b,16#d8,16#ab,16#bb,16#bb,16#9a,
		  16#4a,16#75,16#5a,16#54,16#6a,16#37,16#7a,16#16,16#0a,16#f1,16#1a,16#d0,16#2a,16#b3,16#3a,16#92,
		  16#fd,16#2e,16#ed,16#0f,16#dd,16#6c,16#cd,16#4d,16#bd,16#aa,16#ad,16#8b,16#9d,16#e8,16#8d,16#c9,
		  16#7c,16#26,16#6c,16#07,16#5c,16#64,16#4c,16#45,16#3c,16#a2,16#2c,16#83,16#1c,16#e0,16#0c,16#c1,
		  16#ef,16#1f,16#ff,16#3e,16#cf,16#5d,16#df,16#7c,16#af,16#9b,16#bf,16#ba,16#8f,16#d9,16#9f,16#f8,
		  16#6e,16#17,16#7e,16#36,16#4e,16#55,16#5e,16#74,16#2e,16#93,16#3e,16#b2,16#0e,16#d1,16#1e,16#f0
		>>).

% key1 -> 9189
crc16(Crc, <<>>) ->
    Crc;
crc16(Crc, <<B, Bs/binary>>) ->
    Index = ((Crc bsr 8) bxor B) band 16#ff,
    <<_:(Index * 16), Val:16, _/binary>> = ?CRCTAB,
    crc16((Crc bsl 8) bxor Val, Bs).
