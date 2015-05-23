# Riak Multi-Get

Riak extension that enables efficient multi-key fetch. Internally it relies on `riak_pipe`.

### Features and non-features:

- protobuf interface (piggybacks on riak's PB interface)
- allows to fetch multiple objects at once
	- for JSON objects set of fields in a request may be narrowed down.
- gets the first sibiling from the first available replica
	- thus data may be stale

### TODOs

- HTTP interface

