-module(riak_multiget_pb_SUITE).

-include_lib("common_test/include/ct.hrl").
-include("../include/riak_multiget_pb.hrl").

-export([all/0]).
-export([init_per_suite/1, end_per_suite/1,
         init_per_testcase/2, end_per_testcase/2]).

-export([normal_test/1, 
         stream_test/1,
         multiple_test/1
        ]).

%%% Common Test callbacks 

all() ->
    [
     normal_test,
     stream_test,
     multiple_test
    ].

init_per_suite(Config) ->
    WasThere = application:which_applications(),
    application:load(riak_core),
    application:load(riak_kv),
    application:set_env(riak_core, ring_creation_size, 4),
    application:set_env(riak_core, ring_state_dir, "<nostore>"),
    application:set_env(riak_kv, storage_backend, riak_kv_memory_backend),
    application:set_env(riak_core, default_bucket_props,
                        [
                         {allow_mult, true},
                         {last_write_wins, false}
                        ]),
    application:set_env(riak_api, pb_ip, "0.0.0.0"),
    application:set_env(riak_api, pb_port, 8070),
    application:set_env(riak_kv, key_buffer_size, 5),
    application:set_env(memory_backend, test, true),
    ok = riak_multiget_app:start(),
    riak_kv_memory_backend:reset(),
    WithRiak = application:which_applications(),
    ToShutdown = WithRiak -- WasThere,
    [{to_shutdown, ToShutdown} | Config].

end_per_suite(Config) ->
    riak_kv_memory_backend:reset(),
    [application:stop(A) || 
     {A, _Desc, _Vsn} <- ?config(to_shutdown, Config)],
    ok.

init_per_testcase(_, Config) ->
    {ok, C} = riak:local_client(),
    Obj1 = riak_object:new(<<"foo">>,<<"1">>, <<"{\"a\":1, \"b\":2}">>),
    Obj2 = riak_object:new(<<"foo">>,<<"2">>, <<"{\"a\":1, \"b\":4}">>),
    [C:put(O) || O <- [Obj1, Obj2]],
    {ok, Port} = application:get_env(riak_api, pb_port),
    {ok, Sock} = gen_tcp:connect("localhost", Port, [{packet,4}, binary]),
    [{pb_socket, Sock} | Config].

end_per_testcase(_, Config) ->
    gen_tcp:close(?config(pb_socket, Config)),
    riak_kv_memory_backend:reset(),
    ok.

%%% Tests


normal_test(Config) ->
    check_one_req(false, Config),
    ok.

stream_test(Config) ->
    check_one_req(true, Config),
    ok.

multiple_test(Config) ->
    Sock = ?config(pb_socket, Config),
    Req = #rpbmultigetreq{
                bucket = <<"foo">>,
                keys = [<<"1">>,<<"2">>,<<"3">>]
               },
    send_req(Sock, Req),
    Resp = unify_resp(receive_response(Sock)),
    send_req(Sock, Req#rpbmultigetreq{ stream = true }),
    Resp = unify_resp(receive_response(Sock)),
    send_req(Sock, Req),
    Resp = unify_resp(receive_response(Sock)),
    ok.

%%% Helpers

check_one_req(Stream, Config) ->
    Sock = ?config(pb_socket, Config),
    send_req(Sock, #rpbmultigetreq{
                bucket = <<"foo">>,
                keys = [<<"1">>,<<"2">>,<<"3">>],
                filter_fields = [<<"a">>],
                stream = Stream
               }),
    Resp = receive_response(Sock),
    #rpbmultigetresp{results = [
                                #rpbmultigetkvpair{key = <<"1">>, value = <<"{\"a\":1}">>},
                                #rpbmultigetkvpair{key = <<"2">>, value = <<"{\"a\":1}">>},
                                #rpbmultigetkvpair{key = <<"3">>, value = undefined}
                               ],
                     done = 'OK'} = unify_resp(Resp).

send_req(Sock, #rpbmultigetreq{} = Req) ->
    Msg = riak_multiget_pb:encode(Req),
    ok = gen_tcp:send(Sock, [101, Msg]).

unify_resp(Resp) ->
  Resp#rpbmultigetresp{ results = lists:sort(Resp#rpbmultigetresp.results) }.

receive_response(Sock) ->
    receive_response_loop(Sock, []).

receive_response_loop(Sock, Acc) ->
    case receive_response_once(Sock) of
        timeout -> timeout;
        #rpbmultigetresp{ results = Rs, done = undefined } ->
            receive_response_loop(Sock, Rs ++ Acc);
        #rpbmultigetresp{ results = Rs } = Resp ->
            Resp#rpbmultigetresp{ results = Rs ++ Acc }
    end.

receive_response_once(Sock) ->
    receive {tcp, Sock, <<102, Resp/binary>>} ->
                riak_multiget_pb:decode(rpbmultigetresp, Resp) 
    after 100 -> timeout end.
