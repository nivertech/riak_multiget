-module(riak_multiget_pb_service).

-behaviour(riak_api_pb_service).

-export([init/0,
         decode/2,
         encode/1,
         process/2,
         process_stream/3,
         signature/0
        ]).

-include("../include/riak_multiget_pb.hrl").
-include("../include/riak_multiget_fsm.hrl").

signature() ->
    {?MODULE, 101, 102}.

-record(state, { mon_ref }).
init() ->
    #state{}.

decode(Code, Bin) when Code > 100, Code < 200 ->
    {ok, riak_multiget_pb:decode(msg_type(Code), Bin)}.

encode(Message) ->
    MsgType = element(1, Message),
    {ok, [msg_code(MsgType) | riak_multiget_pb:encode(Message)]}.

process(#rpbmultigetreq{ bucket        = Bucket,
                         keys          = Keys,
                         filter_fields = Fields,
                         timeout       = Timeout,
                         stream        = Stream
                       }, State) ->
    Options = filter_opt(Fields, timeout_opt(Timeout, [])),
    Method = method(Stream),
    case riak_multiget_fsm:Method(Bucket, Keys, Options) of
        {error, Reason} ->
            {error, {format, Reason}};
        {ok, Pid, ReqID} when is_pid(Pid) ->
            MRef = monitor(process, Pid),
            {reply, {stream, ReqID}, State#state{ mon_ref = MRef }};
        {ok, Type, Results} when is_list(Results) ->
            {reply, #rpbmultigetresp{
                       done    = type2status(Type),
                       results = transform_results(Results)
                      }, State}
    end.

process_stream(?MULTIGET_RESULTS(ReqID, []), ReqID, State) ->
    {ignore, State};
process_stream(?MULTIGET_RESULTS(ReqID, Results), ReqID, State) ->
    {reply, #rpbmultigetresp{ results = transform_results(Results) }, State};
process_stream(?MULTIGET_DONE(ReqID, Type), ReqID, State) ->
    demonitor(State#state.mon_ref, [flush]),
    {done, #rpbmultigetresp{ done = type2status(Type) }, State};
process_stream({'DOWN', MRef, process, _Obj, Info}, _,
               #state{ mon_ref = MRef } = State) ->
    {error, {format, {fsm_down, Info}}, State};
process_stream(_, _, State) ->
    {ignore, State}.

%% Helpers

transform_results(Results) ->
    [#rpbmultigetkvpair{
        key = K,
        value = res2pb(V) 
       } || {K, V} <- Results].

method(true = _Stream) -> start_link;
method(false = _Stream) -> run.

filter_opt([], Opts) -> Opts;
filter_opt(Fields, Opts) when is_list(Fields) ->
    [{fields, Fields} | Opts].

timeout_opt(undefined, Opts) -> Opts;
timeout_opt(Timeout, Opts) when is_integer(Timeout) ->
    [{timeout, Timeout} | Opts].

type2status(eoi) -> 'OK';
type2status(timeout) -> 'TIMEOUT'.

res2pb(Value) when is_binary(Value) -> Value;
res2pb(Reason) when is_atom(Reason) -> undefined.

msg_code(rpbmultigetreq) -> 101;
msg_code(rpbmultigetresp) -> 102.

msg_type(101) -> rpbmultigetreq;
msg_type(102) -> rpbmultigetresp.
