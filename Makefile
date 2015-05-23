all: build

REBAR = ./rebar

clean:
	${REBAR} clean
	rm -rf logs
	rm -rf .eunit
	rm -f test/*.beam

depends: 
	${REBAR} get-deps

build: depends
	${REBAR} compile

test: build
	${REBAR} skip_dept=true eunit
	${REBAR} skip_dept=tru ct

tags:
	erl -s tags subdir "./" -s init stop -noshell

