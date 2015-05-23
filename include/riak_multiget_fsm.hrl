-ifndef(RIAK_MULTIGET_FSM).
-define(RIAK_MULTIGET_FSM, true).

-define(MULTIGET_RESULTS(ReqID, KVs), {results, ReqID, KVs}).
-define(MULTIGET_DONE(ReqID, Reason), {done,    ReqID, Reason}).

-endif.

