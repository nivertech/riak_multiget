-module(riak_multiget_app).

-behaviour(application).

-export([start/0]).
%% Application callbacks
-export([start/2, stop/1]).

start() ->
    a_start(riak_multiget, transient).

%% Application callbacks

start(_StartType, _StartArgs) ->
    riak_multiget_sup:start_link().

stop(_State) ->
    ok.

%% Helpers

a_start(App, Type) ->
  start_ok(App, Type, application:start(App, Type)).

start_ok(_App, _Type, ok) -> ok;
start_ok(_App, _Type, {error, {already_started, _App}}) -> ok;
start_ok(App, Type, {error, {not_started, Dep}}) ->
  ok = a_start(Dep, Type),
  a_start(App, Type);

start_ok(App, _Type, {error, Reason}) ->
  erlang:error({app_start_failed, App, Reason}).

