all: build

REBAR = ./rebar

clean:
	${REBAR} clean
	rm -rf logs
	rm -rf .eunit
	rm -f test/*.beam

deps: 
	${REBAR} get-deps compile

build: deps
	${REBAR} skip_deps=true compile

test: build
	${REBAR} skip_deps=true eunit
	-rm -rf logs
	${REBAR} skip_deps=true ct -v1

tags:
	erl -s tags subdir "./" -s init stop -noshell

