-module(riak_multiget_fsm).
-behaviour(gen_fsm).
-define(DEFAULT_TIMEOUT, 5000).

-include("../include/riak_multiget_fsm.hrl").
-include_lib("riak_pipe/include/riak_pipe.hrl").
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.


%% API Function Exports
-export([start_link/3, run/3]).

%% gen_fsm Function Exports
-export([init/1, send_inputs/2, wait_results/2,
         handle_event/3, handle_sync_event/4, 
         handle_info/3, terminate/3, code_change/4]).

-type option() :: {timeout, non_neg_integer()} | {fields, [binary()]}.

%% API Function Definitions

-spec start_link(riak_object:bucket(), [riak_object:key()], [option()]) ->
    {ok, pid(), reference()} | {error, term()}.
start_link(Bucket, Keys, Options)
  when is_binary(Bucket),
       is_list(Keys),
       is_list(Options) ->
    Requester = self(),
    ReqID = make_ref(),
    Options1 = [{bucket,    Bucket},
                {keys,      Keys},
                {requester, Requester},
                {req_id,    ReqID}
                | Options],
    Result = case whereis(riak_multiget_fsm_sj) of
                 undefined ->
                     gen_fsm:start_link(?MODULE, Options1, []);
                 _ ->
                     case sidejob_supervisor:start_child(riak_multiget_fsm_sj,
                                                         gen_fsm, start_link,
                                                         [?MODULE, Options1, []]) of
                         {error, overload} -> {error, overload};
                         {ok, _} = Res     -> Res
                     end
             end,
    case Result of
        {error, _} = Err -> Err;
        {ok, Pid} ->
            {ok, Pid, ReqID}
    end.

-spec run(riak_object:bucket(), [riak_object:key()], [option()]) ->
    {ok, pid(), reference()} | {error, term()}.
run(Bucket, Keys, Options) ->
    collect_results(start_link(Bucket, Keys, Options)).

%% gen_fsm Function Definitions

-record(state, {
          requester :: pid(),
          req_id    :: reference(),
          bucket    :: riak_object:bucket(),
          keys      :: [riak_object:key()],
          fields    :: [binary()],
          timeout   :: timeout(),
          pipe      :: #pipe{}
         }).

init(Args) ->
    State = #state{
               requester = proplists:get_value(requester, Args),
               req_id    = proplists:get_value(req_id,    Args, make_ref()),
               bucket    = proplists:get_value(bucket,    Args, <<>>),
               keys      = proplists:get_value(keys,      Args, []),
               fields    = proplists:get_value(fields,    Args),
               timeout   = proplists:get_value(timeout,   Args, ?DEFAULT_TIMEOUT)
              },
    case riak_multiget_pipe:pipe_start(
           State#state.req_id, State#state.fields) of
        {error, _} = Err ->
            {stop, Err};
        {ok, Pipe} ->
            _MRef = monitor(process, State#state.requester),
            {ok, send_inputs, State#state{ pipe = Pipe }, 0}
    end.

send_inputs(timeout, #state{ bucket = Bucket, keys = Keys, pipe = Pipe } = State) ->
    Failed = [{K, R} || K <- Keys,
                        %% format is `{{Bucket, Key}, KeyData}',
                        %% see `riak_kv_pipe_get' for the details
                        R <- [riak_pipe:queue_work(Pipe, {{Bucket, K}, K}, noblock)],
                        R =/= ok],
    riak_pipe:eoi(Pipe),
    State#state.requester ! ?MULTIGET_RESULTS(State#state.req_id, Failed),
    {next_state, wait_results, State, State#state.timeout}.

wait_results(#pipe_result{ ref = Ref, result = {_Key, _Value} = Result },
             #state{ pipe = #pipe{ sink = #fitting{ref=Ref} } } = State) ->
    State#state.requester ! ?MULTIGET_RESULTS(State#state.req_id, [Result]),
    {next_state, wait_results, State, State#state.timeout};
wait_results(#pipe_eoi{ ref = Ref},
             #state{ pipe = #pipe{ sink = #fitting{ref=Ref} } } = State) ->
    State#state.requester ! ?MULTIGET_DONE(State#state.req_id, eoi),
    {stop, normal, State};
wait_results(timeout, State) ->
    State#state.requester ! ?MULTIGET_DONE(State#state.req_id, timeout),
    {stop, normal, State}.

handle_event(_Event, _StateName, State) ->
    {stop, {unexpected_event, _Event}, State}.

handle_sync_event(_Event, _From, _StateName, State) ->
    {stop, {unexpected_event, _Event}, State}.

handle_info({'DOWN', _MRef, process, Pid, Info}, _StateName,
            #state{ requester = Pid } = State) ->
    {stop, {requester_down, Info}, State};
handle_info(_Info, _StateName, State) ->
    {stop, {unexpected, _Info}, State}.

terminate(_Reason, _StateName, _State) ->
    ok.

code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

%% Internal Function Definitions

collect_results({error, _} = Err) -> Err;
collect_results({ok, Pid, ReqID}) ->
    MRef = monitor(process, Pid),
    collect_results_loop(MRef, ReqID, []).

collect_results_loop(MRef, ReqID, Acc) ->
    receive
        {'DOWN', MRef, process, _Obj, Info} ->
            {error, {fsm_down, Info}};
        ?MULTIGET_RESULTS(ReqID, Results) ->
            collect_results_loop(MRef, ReqID, Results ++ Acc);
        ?MULTIGET_DONE(ReqID, Reason) ->
            demonitor(MRef, [flush]),
            {ok, Reason, Acc}
    end.

%% EUnit tests

-ifdef(TEST).

flush() ->
    receive _ -> flush()
    after 0 -> ok end.

setup_mocks() ->
    flush(),
    meck:expect(riak_pipe, exec, 
                fun(Fittings, _Opts) ->
                        {ok, #pipe{ builder = self(),
                                    fittings = Fittings,
                                    sink = #fitting{ pid = self(), ref = make_ref() }
                                  }}
                end),
    meck:expect(riak_pipe, queue_work,
                fun
                    (_Pipe, {_, <<"error">>}, _Timeout) ->
                        timeout;
                    (Pipe, {{_B, K}, K}, _Timeout) ->
                        gen_fsm:send_event(Pipe#pipe.sink#fitting.pid,
                                           #pipe_result{ ref = Pipe#pipe.sink#fitting.ref,
                                                         result = {K, K},
                                                         from = mock
                                                       }),
                        ok
                end),
    meck:expect(riak_pipe, eoi, 
                fun(Pipe) ->
                        gen_fsm:send_event(Pipe#pipe.sink#fitting.pid,
                                           #pipe_eoi{ ref = Pipe#pipe.sink#fitting.ref })
                end).

fsm_happy_test() ->
    setup_mocks(),
    Keys = [<<($0 + I)>> || I <- lists:seq(1, 9)],
    Expected = lists:reverse([{K, K} || K <- Keys]),
    ?assertEqual({ok, eoi, Expected}, run(<<"foo">>, Keys, [])).

fsm_vnode_busy_test() ->
    setup_mocks(),
    Keys = [<<($0 + I)>> || I <- lists:seq(1, 9)],
    Expected = lists:reverse([{K, K} || K <- Keys]) ++ [{<<"error">>, timeout}],
    ?assertEqual({ok, eoi, Expected}, run(<<"foo">>, [<<"error">> | Keys], [])).

fsm_timeout_test() ->
    setup_mocks(),
    meck:expect(riak_pipe, eoi, fun(_Pipe) -> ok end),
    Keys = [<<($0 + I)>> || I <- lists:seq(1, 9)],
    Expected = lists:reverse([{K, K} || K <- Keys]),
    ?assertEqual({ok, timeout, Expected}, run(<<"foo">>, Keys, [{timeout, 100}])).

-endif.

