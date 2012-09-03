benchmark:
	go test -c ./src/sparsetable
	./sparsetable.test -test.v -test.bench="Sparse.*" -test.benchtime=5 -test.cpuprofile="cpu.out" -test.memprofile="mem.out"
	go tool pprof --text --lines ./sparsetable.test ./cpu.out
	go tool pprof --text --lines ./sparsetable.test ./mem.out
	rm *.out
	rm *.test

test:
	go test sparsetable
	go test posting

run:
	go build -v -o ./bin/superfastmatch superfastmatch
	./bin/superfastmatch
