# Riak Multi-Get
 
Riak extension that enables efficient multi-key fetch. Internally it relies on `riak_pipe`.

### Features and non-features:

- Allows to fetch multiple objects at once
	- for JSON objects set of fields in a request may be narrowed down.
- Protobuf interface (piggybacks on riak's PB interface).
- Gets the first sibiling from the first available replica
	- thus data may be stale.

### TODOs

- HTTP interface

## Examples

To build a standalone version just issue `make`. 

Open the first console and cd into the app's directory, then`$ ./start.sh`,
wait a bit until everything starts up, then (in the erlang console) issue `node()`

Open the second one and do the`$ ./start.sh 2`, wait a bit again, then type `riak_core:join('first_node@name')` where `first_node@name` is the thing returned by `node()` in the first console.

You can join up to 9 nodes in the same way.

### Ruby

```
cd examples/ruby
bundle install
bundle exec ruby main.rb 
```

## Injection into Riak installed from package

1. Clone this repo.
2. Issue `make with_riak_erl PATH_TO_RIAK=/usr/local/...` where `PATH_TO_RIAK` should be a path to your riak installation.
3. To your `vm.args` file add the following:

    ```
    -pa /path/too/riak_multiget/ebin
    -s riak_multiget_app
    ```



