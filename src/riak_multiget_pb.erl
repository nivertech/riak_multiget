-file("src/riak_multiget_pb.erl", 1).

-module(riak_multiget_pb).

-export([encode_rpbmultigetresp/1,
	 decode_rpbmultigetresp/1,
	 delimited_decode_rpbmultigetresp/1,
	 encode_rpbmultigetkvpair/1, decode_rpbmultigetkvpair/1,
	 delimited_decode_rpbmultigetkvpair/1,
	 encode_rpbmultigetreq/1, decode_rpbmultigetreq/1,
	 delimited_decode_rpbmultigetreq/1]).

-export([has_extension/2, extension_size/1,
	 get_extension/2, set_extension/3]).

-export([decode_extensions/1]).

-export([encode/1, decode/2, delimited_decode/2]).

-export([int_to_enum/2, enum_to_int/2]).

-record(rpbmultigetresp, {results, done}).

-record(rpbmultigetkvpair, {key, value}).

-record(rpbmultigetreq,
	{bucket, keys, filter_fields, timeout, stream}).

encode([]) -> [];
encode(Records) when is_list(Records) ->
    delimited_encode(Records);
encode(Record) -> encode(element(1, Record), Record).

encode_rpbmultigetresp(Records) when is_list(Records) ->
    delimited_encode(Records);
encode_rpbmultigetresp(Record)
    when is_record(Record, rpbmultigetresp) ->
    encode(rpbmultigetresp, Record).

encode_rpbmultigetkvpair(Records)
    when is_list(Records) ->
    delimited_encode(Records);
encode_rpbmultigetkvpair(Record)
    when is_record(Record, rpbmultigetkvpair) ->
    encode(rpbmultigetkvpair, Record).

encode_rpbmultigetreq(Records) when is_list(Records) ->
    delimited_encode(Records);
encode_rpbmultigetreq(Record)
    when is_record(Record, rpbmultigetreq) ->
    encode(rpbmultigetreq, Record).

encode(rpbmultigetreq, Records) when is_list(Records) ->
    delimited_encode(Records);
encode(rpbmultigetreq, Record) ->
    [iolist(rpbmultigetreq, Record)
     | encode_extensions(Record)];
encode(rpbmultigetkvpair, Records)
    when is_list(Records) ->
    delimited_encode(Records);
encode(rpbmultigetkvpair, Record) ->
    [iolist(rpbmultigetkvpair, Record)
     | encode_extensions(Record)];
encode(rpbmultigetresp, Records)
    when is_list(Records) ->
    delimited_encode(Records);
encode(rpbmultigetresp, Record) ->
    [iolist(rpbmultigetresp, Record)
     | encode_extensions(Record)].

encode_extensions(_) -> [].

delimited_encode(Records) ->
    lists:map(fun (Record) ->
		      IoRec = encode(Record),
		      Size = iolist_size(IoRec),
		      [protobuffs:encode_varint(Size), IoRec]
	      end,
	      Records).

iolist(rpbmultigetreq, Record) ->
    [pack(1, required,
	  with_default(Record#rpbmultigetreq.bucket, none), bytes,
	  []),
     pack(2, repeated,
	  with_default(Record#rpbmultigetreq.keys, none), bytes,
	  []),
     pack(3, repeated,
	  with_default(Record#rpbmultigetreq.filter_fields, none),
	  bytes, []),
     pack(4, optional,
	  with_default(Record#rpbmultigetreq.timeout, none),
	  uint32, []),
     pack(5, optional,
	  with_default(Record#rpbmultigetreq.stream, false), bool,
	  [])];
iolist(rpbmultigetkvpair, Record) ->
    [pack(1, required,
	  with_default(Record#rpbmultigetkvpair.key, none), bytes,
	  []),
     pack(2, optional,
	  with_default(Record#rpbmultigetkvpair.value, none),
	  bytes, [])];
iolist(rpbmultigetresp, Record) ->
    [pack(1, repeated,
	  with_default(Record#rpbmultigetresp.results, none),
	  rpbmultigetkvpair, []),
     pack(2, optional,
	  with_default(Record#rpbmultigetresp.done, none),
	  rpbmultigetstatus, [])].

with_default(Default, Default) -> undefined;
with_default(Val, _) -> Val.

pack(_, optional, undefined, _, _) -> [];
pack(_, repeated, undefined, _, _) -> [];
pack(_, repeated_packed, undefined, _, _) -> [];
pack(_, repeated_packed, [], _, _) -> [];
pack(FNum, required, undefined, Type, _) ->
    exit({error,
	  {required_field_is_undefined, FNum, Type}});
pack(_, repeated, [], _, Acc) -> lists:reverse(Acc);
pack(FNum, repeated, [Head | Tail], Type, Acc) ->
    pack(FNum, repeated, Tail, Type,
	 [pack(FNum, optional, Head, Type, []) | Acc]);
pack(FNum, repeated_packed, Data, Type, _) ->
    protobuffs:encode_packed(FNum, Data, Type);
pack(FNum, _, Data, _, _) when is_tuple(Data) ->
    [RecName | _] = tuple_to_list(Data),
    protobuffs:encode(FNum, encode(RecName, Data), bytes);
pack(FNum, _, Data, Type, _)
    when Type =:= bool;
	 Type =:= int32;
	 Type =:= uint32;
	 Type =:= int64;
	 Type =:= uint64;
	 Type =:= sint32;
	 Type =:= sint64;
	 Type =:= fixed32;
	 Type =:= sfixed32;
	 Type =:= fixed64;
	 Type =:= sfixed64;
	 Type =:= string;
	 Type =:= bytes;
	 Type =:= float;
	 Type =:= double ->
    protobuffs:encode(FNum, Data, Type);
pack(FNum, _, Data, Type, _) when is_atom(Data) ->
    protobuffs:encode(FNum, enum_to_int(Type, Data), enum).

enum_to_int(rpbmultigetstatus, 'TIMEOUT') -> 2;
enum_to_int(rpbmultigetstatus, 'OK') -> 1.

int_to_enum(rpbmultigetstatus, 2) -> 'TIMEOUT';
int_to_enum(rpbmultigetstatus, 1) -> 'OK';
int_to_enum(_, Val) -> Val.

decode_rpbmultigetresp(Bytes) when is_binary(Bytes) ->
    decode(rpbmultigetresp, Bytes).

decode_rpbmultigetkvpair(Bytes) when is_binary(Bytes) ->
    decode(rpbmultigetkvpair, Bytes).

decode_rpbmultigetreq(Bytes) when is_binary(Bytes) ->
    decode(rpbmultigetreq, Bytes).

delimited_decode_rpbmultigetreq(Bytes) ->
    delimited_decode(rpbmultigetreq, Bytes).

delimited_decode_rpbmultigetkvpair(Bytes) ->
    delimited_decode(rpbmultigetkvpair, Bytes).

delimited_decode_rpbmultigetresp(Bytes) ->
    delimited_decode(rpbmultigetresp, Bytes).

delimited_decode(Type, Bytes) when is_binary(Bytes) ->
    delimited_decode(Type, Bytes, []).

delimited_decode(_Type, <<>>, Acc) ->
    {lists:reverse(Acc), <<>>};
delimited_decode(Type, Bytes, Acc) ->
    try protobuffs:decode_varint(Bytes) of
      {Size, Rest} when size(Rest) < Size ->
	  {lists:reverse(Acc), Bytes};
      {Size, Rest} ->
	  <<MessageBytes:Size/binary, Rest2/binary>> = Rest,
	  Message = decode(Type, MessageBytes),
	  delimited_decode(Type, Rest2, [Message | Acc])
    catch
      _What:_Why -> {lists:reverse(Acc), Bytes}
    end.

decode(enummsg_values, 1) -> value1;
decode(rpbmultigetreq, Bytes) when is_binary(Bytes) ->
    Types = [{5, stream, bool, []},
	     {4, timeout, uint32, []},
	     {3, filter_fields, bytes, [repeated]},
	     {2, keys, bytes, [repeated]}, {1, bucket, bytes, []}],
    Defaults = [{2, keys, []}, {3, filter_fields, []},
		{5, stream, false}],
    Decoded = decode(Bytes, Types, Defaults),
    to_record(rpbmultigetreq, Decoded);
decode(rpbmultigetkvpair, Bytes)
    when is_binary(Bytes) ->
    Types = [{2, value, bytes, []}, {1, key, bytes, []}],
    Defaults = [],
    Decoded = decode(Bytes, Types, Defaults),
    to_record(rpbmultigetkvpair, Decoded);
decode(rpbmultigetresp, Bytes) when is_binary(Bytes) ->
    Types = [{2, done, rpbmultigetstatus, []},
	     {1, results, rpbmultigetkvpair, [is_record, repeated]}],
    Defaults = [{1, results, []}],
    Decoded = decode(Bytes, Types, Defaults),
    to_record(rpbmultigetresp, Decoded).

decode(<<>>, Types, Acc) ->
    reverse_repeated_fields(Acc, Types);
decode(Bytes, Types, Acc) ->
    {ok, FNum} = protobuffs:next_field_num(Bytes),
    case lists:keyfind(FNum, 1, Types) of
      {FNum, Name, Type, Opts} ->
	  {Value1, Rest1} = case lists:member(is_record, Opts) of
			      true ->
				  {{FNum, V}, R} = protobuffs:decode(Bytes,
								     bytes),
				  RecVal = decode(Type, V),
				  {RecVal, R};
			      false ->
				  case lists:member(repeated_packed, Opts) of
				    true ->
					{{FNum, V}, R} =
					    protobuffs:decode_packed(Bytes,
								     Type),
					{V, R};
				    false ->
					{{FNum, V}, R} =
					    protobuffs:decode(Bytes, Type),
					{unpack_value(V, Type), R}
				  end
			    end,
	  case lists:member(repeated, Opts) of
	    true ->
		case lists:keytake(FNum, 1, Acc) of
		  {value, {FNum, Name, List}, Acc1} ->
		      decode(Rest1, Types,
			     [{FNum, Name, [int_to_enum(Type, Value1) | List]}
			      | Acc1]);
		  false ->
		      decode(Rest1, Types,
			     [{FNum, Name, [int_to_enum(Type, Value1)]} | Acc])
		end;
	    false ->
		decode(Rest1, Types,
		       [{FNum, Name, int_to_enum(Type, Value1)} | Acc])
	  end;
      false ->
	  case lists:keyfind('$extensions', 2, Acc) of
	    {_, _, Dict} ->
		{{FNum, _V}, R} = protobuffs:decode(Bytes, bytes),
		Diff = size(Bytes) - size(R),
		<<V:Diff/binary, _/binary>> = Bytes,
		NewDict = dict:store(FNum, V, Dict),
		NewAcc = lists:keyreplace('$extensions', 2, Acc,
					  {false, '$extensions', NewDict}),
		decode(R, Types, NewAcc);
	    _ ->
		{ok, Skipped} = protobuffs:skip_next_field(Bytes),
		decode(Skipped, Types, Acc)
	  end
    end.

reverse_repeated_fields(FieldList, Types) ->
    [begin
       case lists:keyfind(FNum, 1, Types) of
	 {FNum, Name, _Type, Opts} ->
	     case lists:member(repeated, Opts) of
	       true -> {FNum, Name, lists:reverse(Value)};
	       _ -> Field
	     end;
	 _ -> Field
       end
     end
     || {FNum, Name, Value} = Field <- FieldList].

unpack_value(Binary, string) when is_binary(Binary) ->
    binary_to_list(Binary);
unpack_value(Value, _) -> Value.

to_record(rpbmultigetreq, DecodedTuples) ->
    Record1 = lists:foldr(fun ({_FNum, Name, Val},
			       Record) ->
				  set_record_field(record_info(fields,
							       rpbmultigetreq),
						   Record, Name, Val)
			  end,
			  #rpbmultigetreq{}, DecodedTuples),
    Record1;
to_record(rpbmultigetkvpair, DecodedTuples) ->
    Record1 = lists:foldr(fun ({_FNum, Name, Val},
			       Record) ->
				  set_record_field(record_info(fields,
							       rpbmultigetkvpair),
						   Record, Name, Val)
			  end,
			  #rpbmultigetkvpair{}, DecodedTuples),
    Record1;
to_record(rpbmultigetresp, DecodedTuples) ->
    Record1 = lists:foldr(fun ({_FNum, Name, Val},
			       Record) ->
				  set_record_field(record_info(fields,
							       rpbmultigetresp),
						   Record, Name, Val)
			  end,
			  #rpbmultigetresp{}, DecodedTuples),
    Record1.

decode_extensions(Record) -> Record.

decode_extensions(_Types, [], Acc) ->
    dict:from_list(Acc);
decode_extensions(Types, [{Fnum, Bytes} | Tail], Acc) ->
    NewAcc = case lists:keyfind(Fnum, 1, Types) of
	       {Fnum, Name, Type, Opts} ->
		   {Value1, Rest1} = case lists:member(is_record, Opts) of
				       true ->
					   {{FNum, V}, R} =
					       protobuffs:decode(Bytes, bytes),
					   RecVal = decode(Type, V),
					   {RecVal, R};
				       false ->
					   case lists:member(repeated_packed,
							     Opts)
					       of
					     true ->
						 {{FNum, V}, R} =
						     protobuffs:decode_packed(Bytes,
									      Type),
						 {V, R};
					     false ->
						 {{FNum, V}, R} =
						     protobuffs:decode(Bytes,
								       Type),
						 {unpack_value(V, Type), R}
					   end
				     end,
		   case lists:member(repeated, Opts) of
		     true ->
			 case lists:keytake(FNum, 1, Acc) of
			   {value, {FNum, Name, List}, Acc1} ->
			       decode(Rest1, Types,
				      [{FNum, Name,
					lists:reverse([int_to_enum(Type, Value1)
						       | lists:reverse(List)])}
				       | Acc1]);
			   false ->
			       decode(Rest1, Types,
				      [{FNum, Name, [int_to_enum(Type, Value1)]}
				       | Acc])
			 end;
		     false ->
			 [{Fnum,
			   {optional, int_to_enum(Type, Value1), Type, Opts}}
			  | Acc]
		   end;
	       false -> [{Fnum, Bytes} | Acc]
	     end,
    decode_extensions(Types, Tail, NewAcc).

set_record_field(Fields, Record, '$extensions',
		 Value) ->
    Decodable = [],
    NewValue = decode_extensions(element(1, Record),
				 Decodable, dict:to_list(Value)),
    Index = list_index('$extensions', Fields),
    erlang:setelement(Index + 1, Record, NewValue);
set_record_field(Fields, Record, Field, Value) ->
    Index = list_index(Field, Fields),
    erlang:setelement(Index + 1, Record, Value).

list_index(Target, List) -> list_index(Target, List, 1).

list_index(Target, [Target | _], Index) -> Index;
list_index(Target, [_ | Tail], Index) ->
    list_index(Target, Tail, Index + 1);
list_index(_, [], _) -> -1.

extension_size(_) -> 0.

has_extension(_Record, _FieldName) -> false.

get_extension(_Record, _FieldName) -> undefined.

set_extension(Record, _, _) -> {error, Record}.

