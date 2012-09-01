benchmark:
	go test -c
	./pkg.test -test.v -test.bench="Sparse.*" -test.benchtime=1 -test.cpuprofile="cpu.out" -test.memprofile="mem.out"
	go tool pprof --text --lines ./pkg.test ./cpu.out
	go tool pprof --text --lines ./pkg.test ./mem.out

test:
	go test

run:
	go run main.go