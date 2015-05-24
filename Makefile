REBAR = ./rebar
PATH_TO_RIAK ?= /usr/local/Cellar/riak132/1.3.2/libexec

all: build

clean:
	${REBAR} clean
	rm -rf logs
	rm -rf .eunit
	rm -f test/*.beam

deps: 
	${REBAR} get-deps compile

build: deps src/riak_multiget_pb.erl
	${REBAR} skip_deps=true compile

test: build
	${REBAR} skip_deps=true eunit
	-rm -rf logs
	${REBAR} skip_deps=true ct -v1

tags:
	erl -s tags subdir "./" -s init stop -noshell

# This is necessary to make `with_riak_erl` useful
src/riak_multiget_pb.erl: deps
	erl -pa deps/*/ebin -eval 'protobuffs_compile:generate_source("src/riak_multiget.proto", [{output_src_dir, "src"}, {output_include_dir, "include"}]), init:stop().'

with_riak_erl:
	${PATH_TO_RIAK}/erts-5.9.1/bin/erl -make
