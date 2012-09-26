package testutils

import (
	"bufio"
	. "launchpad.net/gocheck"
	"os"
	"os/exec"
	"registry"
	"strings"
	"syscall"
)

type DBSuite struct {
	Registry *registry.Registry
	mongoCmd *exec.Cmd
	c        *C
}

func (s *DBSuite) killMongo() {
	s.mongoCmd.Process.Signal(syscall.SIGINT)
	s.mongoCmd.Wait()
}

func (s *DBSuite) startMongo() {
	dbPath := s.c.MkDir()
	s.mongoCmd = exec.Command(os.Getenv("GOPATH")+"/mongo/bin/mongod", "--dbpath", dbPath, "--port", "12345", "--rest")
	stdout, err := s.mongoCmd.StdoutPipe()
	s.c.Assert(err, IsNil)
	outBuf := bufio.NewReader(stdout)
	s.c.Assert(s.mongoCmd.Start(), IsNil)
	for {
		outLine, outErr := outBuf.ReadString('\n')
		s.c.Assert(outErr, IsNil)
		s.c.Log(outLine)
		if strings.Contains(outLine, "waiting for connections") {
			break
		}
	}
	s.c.Log("Started mongo test instance")
}

func (s *DBSuite) SetUpSuite(c *C) {
	s.c = c
	s.Registry = registry.NewRegistry([]string{"-db=test", "-mongo_url=localhost:12345", "-api_address=localhost:9080", "-posting_addresses=localhost:9090,localhost:9091"})
	s.startMongo()
}

func (s *DBSuite) TearDownSuite(c *C) {
	s.killMongo()
}

func (s *DBSuite) SetUpTest(c *C) {
	s.c.Log("Opening Registry")
	s.Registry.Open()
	s.c.Log("Dropping Test Database")
	s.Registry.DropDatabase()
}

func (s *DBSuite) TearDownTest(c *C) {
	s.c.Log("Closing Registry")
	s.Registry.Close()
}
