-module(riak_multiget_app).

-behaviour(application).

-export([start/0]).
%% Application callbacks
-export([start/2, prep_stop/1, stop/1]).

start() ->
    a_start(riak_multiget, transient).

%% Application callbacks

start(_StartType, _StartArgs) ->
    riak_core:wait_for_service(riak_kv),
    case riak_multiget_sup:start_link() of
        {error, _} = Err -> Err;
        {ok, _} = Sup ->
            ok = riak_api_pb_service:register(pb_services()),
            Sup
    end.

prep_stop(_State) ->
    try
        ok = riak_api_pb_service:deregister(pb_services()),
        lager:info("Unregistered pb services")
    catch
        Type:Reason ->
            lager:error("Stopping application riak_multiget - ~p:~p.\n", [Type, Reason])
    end,
    stopping.

stop(_State) ->
    ok.

%% Helpers

pb_services() ->
    [Mod:signature() || Mod <- [riak_multiget_pb_service]].

a_start(App, Type) ->
  start_ok(App, Type, application:start(App, Type)).

start_ok(_App, _Type, ok) -> ok;
start_ok(_App, _Type, {error, {already_started, _App}}) -> ok;
start_ok(App, Type, {error, {not_started, Dep}}) ->
  ok = a_start(Dep, Type),
  a_start(App, Type);

start_ok(App, _Type, {error, Reason}) ->
  erlang:error({app_start_failed, App, Reason}).

