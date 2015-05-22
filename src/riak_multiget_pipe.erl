%%%-------------------------------------------------------------------
%%% @author Valery Meleshkin <valery.meleshkin@wooga.com>
%%% @copyright 2015 Wooga
%%%-------------------------------------------------------------------

-module(riak_multiget_pipe).

-include_lib("riak_pipe/include/riak_pipe.hrl").

-export([run/3, run/6]).

%% Internal exports
-export([pick_xform/3]).

-export([init/1, result/3, finish/2]).

-type fields_spec() :: [binary()] | undefined.

-callback init(Arg::term()) -> {ok, State::term()}.
-callback result(riak_object:key(), binary() | notfound | timeout, State::term()) -> 
    {ok, State::term()} | {stop, State::term()}.
-callback finish(eoi | timeout | stopped, State::term()) -> term().

init(_) -> {ok, []}.
result(K, V, Acc) ->
    {ok, [{K, V} | Acc]}.
finish(Type, Acc) ->
    {Type, Acc}.

run(Bucket, Keys, Fields) ->
    run(?MODULE, [], Bucket, Keys, Fields, 5000).

-spec run(module(), term(),
          riak_object:bucket(), riak_object:key(),
          fields_spec(), timeout()) ->
    ok | {error, term()}.
run(Module, Arg, Bucket, Keys, Fields, Timeout) ->
    case run_(Bucket, Keys, Fields) of
        {error, _} = Err -> Err;
        {ok, Pipe, Failed} ->
            ModRes = lists:foldl(fun({K, V}, {ok, St}) ->
                                         Module:result(K, V, St);
                                    (_, {stop, St}) ->
                                         {stopped, Module:finish(stopped, St)}
                                 end, Module:init(Arg), Failed),
            case ModRes of
                {stopped, Res} -> Res;
                {ok, State1} ->
                    collect_results(Module, State1, Pipe, Timeout)
            end
    end.

-spec collect_results(module(), term(), riak_pipe:pipe(), timeout()) -> term().
collect_results(Module, State, Pipe, Timeout) ->
    case riak_pipe:receive_result(Pipe, Timeout) of
        {result, {_From, {Key, Value}}} ->
            case Module:result(Key, Value, State) of
                {stop, State1} ->
                    Module:finish(stopped, State1);
                {ok, State1} ->
                    collect_results(Module, State1, Pipe, Timeout)
            end;
        {log, {From, Result}} ->
            lager:warning("Log entry ~p from worker: ~p", [Result, From]),
            collect_results(Module, State, Pipe, Timeout);
        End ->
            Module:finish(End, State)
    end.

run_(Bucket, Keys, Fields) ->
    case pipe_start(Bucket, Keys, Fields) of
        {error, _} = Err -> Err;
        {ok, Pipe} ->
            Failed = [{K, R} || K <- Keys,
                                %% format is `{{Bucket, Key}, KeyData}',
                                %% see `riak_kv_pipe_get' for the details
                                R <- [riak_pipe:queue_work(Pipe, {{Bucket, K}, K}, noblock)],
                                R =/= ok],
            riak_pipe:eoi(Pipe),
            {ok, Pipe, Failed}
    end.

-spec pipe_start(riak_object:bucket(), riak_object:key(), fields_spec()) ->
    {ok, riak_pipe:pipe()} | {error, term()}.
pipe_start(Bucket, Keys, Fields) ->
    Sig = {Bucket, Keys, Fields},
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
      [{log, sasl}, {trace, [error]}]).

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

