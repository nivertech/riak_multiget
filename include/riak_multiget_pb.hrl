-ifndef(RPBMULTIGETREQ_PB_H).
-define(RPBMULTIGETREQ_PB_H, true).
-record(rpbmultigetreq, {
    bucket = erlang:error({required, bucket}),
    keys = [],
    filter_fields = [],
    timeout
}).
-endif.

-ifndef(RPBMULTIGETKVPAIR_PB_H).
-define(RPBMULTIGETKVPAIR_PB_H, true).
-record(rpbmultigetkvpair, {
    key = erlang:error({required, key}),
    value
}).
-endif.

-ifndef(RPBMULTIGETRESP_PB_H).
-define(RPBMULTIGETRESP_PB_H, true).
-record(rpbmultigetresp, {
    results = [],
    done
}).
-endif.

