PKGS=$(notdir $(wildcard src/*))

run:
	go install -v superfastmatch
	go build -v -o  ./bin/superfastmatch superfastmatch
	./bin/superfastmatch

dist:
	bash -c "source scripts/crosscompile.bash && go-windows-amd64 build -o builds/superfastmatch.exe superfastmatch"
	bash -c "source scripts/crosscompile.bash && go-darwin-amd64 build -o builds/superfastmatch-darwin superfastmatch"
	bash -c "source scripts/crosscompile.bash && go-linux-amd64 build -o builds/superfastmatch-linux superfastmatch"

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
	rm -f test.log
	mkdir -p data/test/
	mongo/bin/mongod --fork --logpath=test.log --dbpath=data/test/
	go test -v ./... #Fancy way of testing all packages
	# @$(foreach test,$(PKGS),go test -v $(test);)
	mongo/bin/mongo admin --eval "db.shutdownServer()"

dependencies:
	go get -v superfastmatch
	go get -v -t registry

