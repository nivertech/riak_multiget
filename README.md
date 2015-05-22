# Riak Multi-Get

Riak extension that enables efficient multi-key fetch.

Features and non-features:
- allows to fetch whole objects or only certain keys of JSON
- gets the first sibiling from the first available replica (thus data may be stale)

TODO
- protobuf interface
- HTTP interface
