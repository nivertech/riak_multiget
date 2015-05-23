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

signature() ->
    {?MODULE, 101, 102}.

init() ->
    undefined.

decode(Code, Bin) when Code > 100, Code < 200 ->
    {ok, riak_multiget_pb:decode(msg_type(Code), Bin)}.

encode(Message) ->
    MsgType = element(1, Message),
    {ok, [msg_code(MsgType) | riak_multiget_pb:encode(Message)]}.

%% TODO make it stream
process(#rpbmultigetreq{ bucket        = Bucket,
                         keys          = Keys,
                         filter_fields = Fields,
                         timeout       = Timeout
                       }, State) ->
    Fields1 = case Fields of
                  []    -> undefined;
                  [_|_] -> Fields
              end,
    Resp = case riak_multiget_pipe:run(Bucket, Keys, Fields1, Timeout) of
               {error, _} ->
                   #rpbmultigetresp{ done = 'ERROR' };
               {ok, {Type, Results}} ->
                   ResultsPB = [#rpbmultigetkvpair{ key = K, value = res2pb(V) }
                                || {K, V} <- Results],
                   #rpbmultigetresp{
                      done    = type2status(Type),
                      results = ResultsPB
                     }
           end,
    {reply, Resp, State}.

process_stream(_,_,State) ->
    {ignore, State}.

%% Helpers

type2status(eoi) -> 'OK';
type2status(timeout) -> 'TIMEOUT'.

res2pb(Value) when is_binary(Value) -> Value;
res2pb(Reason) when is_atom(Reason) -> undefined.

msg_code(rpbmultigetreq) -> 101;
msg_code(rpbmultigetresp) -> 102.

msg_type(101) -> rpbmultigetreq;
msg_type(102) -> rpbmultigetresp.
