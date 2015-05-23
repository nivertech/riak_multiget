-module(riak_multiget_pipe).

-include_lib("riak_pipe/include/riak_pipe.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export_type([fields_spec/0]).

%% Internal exports
-export([pipe_start/2, pick_xform/3]).

-type fields_spec() :: [binary()] | undefined.

%% Pipe declaration and logic

-spec pipe_start(term(), fields_spec()) ->
    {ok, riak_pipe:pipe()} | {error, term()}.
pipe_start(PipeId, Fields) ->
    Sig = {PipeId, Fields},
    riak_pipe:exec(
      [#fitting_spec{name     = {multiget_get, Sig},
                     module   = riak_kv_pipe_get,
                     chashfun = {riak_kv_pipe_get, bkey_chash},
                     nval     = {riak_kv_pipe_get, bkey_nval}},

       #fitting_spec{arg      = {{modfun, ?MODULE, pick_xform}, Fields},
                     name     = {multiget_pick_fields, Sig},
                     module   = riak_kv_mrc_map,
                     chashfun = follow}
       %pre-reduce umerge local
      ],
      [{log, lager},
       {sink_type, {fsm, infinity, 0}},
       {trace, [error]}
      ]).

-spec pick_xform({error, notfound} | riak_object:riak_object(),
                 riak_object:key(), 
                 fields_spec()) ->
    [{riak_object:key(), binary()}].
pick_xform({error, notfound}, KeyData, _Fields) -> 
    [{KeyData, notfound}];
pick_xform(Obj, KeyData, Fields) -> 
    case riak_object:get_values(Obj) of
        [] -> [{KeyData, notfound}];
        [Val | _] ->
            [{KeyData, pick_fields(Val, Fields)}]
    end.

-spec pick_fields(binary(), fields_spec()) -> binary().
pick_fields(Value, undefined) ->
    Value;
pick_fields(Value, Fields) when is_list(Fields) ->
    JSONTerm = case mochijson2:decode(Value) of
                   {struct, JSONDict} ->
                       {struct, [Field || {K, _V} = Field <- JSONDict,
                                          lists:member(K, Fields)]};
                   Else ->
                       Else
               end,
    iolist_to_binary(mochijson2:encode(JSONTerm)).

%% EUnit tests

-ifdef(TEST).

pick_fields_test() ->
    JSONBin = <<"{\"a\": 1, \"b\": 2}">>,
    JSONBin = pick_fields(JSONBin, undefined),
    <<"{}">> = pick_fields(JSONBin, []),
    <<"{\"a\":1}">> = pick_fields(JSONBin, [<<"a">>]),
    <<"[{\"a\":1,\"b\":2}]">> = pick_fields(<<$[, JSONBin/binary, $]>>, [<<"a">>]).

pick_xform_test() ->
    Key = <<"Key">>,
    [{Key, notfound}] = pick_xform({error, notfound}, Key, undefined),
    Obj0 = riak_object:new(<<"bucket">>, Key, <<>>),
    Obj1 = riak_object:set_contents(Obj0, []),
    Obj2 = riak_object:set_contents(Obj0, [{dict:new(), <<"1">>},
                                           {dict:new(), <<"2">>}]),
    [{Key, notfound}] = pick_xform(Obj1, Key, undefined),
    [{Key, <<"1">>}] = pick_xform(Obj2, Key, undefined).

-endif.

