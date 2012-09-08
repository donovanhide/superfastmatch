run:
	go install -v superfastmatch
	go build -v -o ./bin/superfastmatch superfastmatch
	./bin/superfastmatch

algo: 
	go test -x -c document
	mv document.test src/document
	cd src/document && ./document.test -test.bench="Benchmark.*" -test.cpuprofile="cpu.out"
	cd src/document && go tool pprof --text --lines ./document.test ./cpu.out
	cd src/document && rm *.out
	cd src/document && rm *.test

benchmark:
	go test -c ./src/sparsetable
	./sparsetable.test -test.bench="Benchmark.*" -test.cpuprofile="cpu.out"
	./sparsetable.test -test.bench="Benchmark.*" -test.memprofilerate=1 -test.memprofile="mem.out"
	go tool pprof --text --lines ./sparsetable.test ./cpu.out
	go tool pprof --text --lines ./sparsetable.test ./mem.out
	rm *.out
	rm *.test

test: 
	go test sparsetable
	go test posting
	go test document
	go test query

dependencies:
	go get -u launchpad.net/gocheck
	go get -u code.google.com/p/gorilla/mux
	go get -u labix.org/v2/mgo