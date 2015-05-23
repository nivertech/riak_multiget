# Riak Multi-Get

Riak extension that enables efficient multi-key fetch.

Features and non-features:
- allows to fetch whole objects or only certain keys of JSON
- gets the first sibiling from the first available replica (thus data may be stale)

TODO
- protobuf interface
- HTTP interface

## Sample

```
(riak_multiget1@sumerman-mbpr13.local)7> {ok, C} = riak:local_client().
...
(riak_multiget1@sumerman-mbpr13.local)8> f(O),O = riak_object:new(<<"foo">>,<<"1">>, <<"{\"a\":1, \"b\":2}">>).
...
(riak_multiget1@sumerman-mbpr13.local)9> C:put(O).
ok 
(riak_multiget1@sumerman-mbpr13.local)10> f(O),O = riak_object:new(<<"foo">>,<<"2">>, <<"{\"a\":1, \"b\":2}">>).
...
(riak_multiget1@sumerman-mbpr13.local)11> C:put(O).                                                             
ok
(riak_multiget1@sumerman-mbpr13.local)12> rr(riak_multiget_pb), f(Sock), {ok, Sock} = gen_tcp:connect("localhost", 8071, [{packet,4},binary]), f(Msg), Msg = riak_multiget_pb:encode(#rpbmultigetreq{bucket = <<"foo">>, keys = [<<"1">>,<<"2">>,<<"3">>]}), gen_tcp:send(Sock, [101, Msg]).
ok
(riak_multiget1@sumerman-mbpr13.local)13> f(Resp),receive {tcp, _, <<102, Resp/binary>>} -> riak_multiget_pb:decode(rpbmultigetresp,Resp) after 100 -> timeout end.
#rpbmultigetresp{results = [#rpbmultigetkvpair{key = <<"3">>,                                                          
                                               value = undefined},
                            #rpbmultigetkvpair{key = <<"2">>,
                                               value = <<"{\"a\":1, \"b\":2}">>},
                            #rpbmultigetkvpair{key = <<"1">>,
                                               value = <<"{\"a\":1, \"b\":2}">>}],
                 done = 'OK'}

(riak_multiget1@sumerman-mbpr13.local)14> rr(riak_multiget_pb), f(Sock), {ok, Sock} = gen_tcp:connect("localhost", 8071, [{packet,4},binary]), f(Msg), Msg = riak_multiget_pb:encode(#rpbmultigetreq{bucket = <<"foo">>, keys = [<<"1">>,<<"2">>,<<"3">>], filter_fields = [<<"a">>] }), gen_tcp:send(Sock, [101, Msg]).
ok
(riak_multiget1@sumerman-mbpr13.local)15> f(Resp),receive {tcp, _, <<102, Resp/binary>>} -> riak_multiget_pb:decode(rpbmultigetresp,Resp) after 100 -> timeout end.
#rpbmultigetresp{results = [#rpbmultigetkvpair{key = <<"3">>,                                                                                      
                                               value = undefined},
                            #rpbmultigetkvpair{key = <<"2">>,value = <<"{\"a\":1}">>},
                            #rpbmultigetkvpair{key = <<"1">>,value = <<"{\"a\":1}">>}],
                 done = 'OK'}
```
