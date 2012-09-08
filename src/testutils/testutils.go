package testutils

import (
	"bufio"
	"labix.org/v2/mgo"
	. "launchpad.net/gocheck"
	"os"
	"os/exec"
	"strings"
	"syscall"
)

type DBSuite struct {
	cmd     *exec.Cmd
	session *mgo.Session
	Db      *mgo.Database
}

func (s *DBSuite) SetUpSuite(c *C) {
	dbPath := c.MkDir()
	s.cmd = exec.Command(os.Getenv("GOPATH")+"/mongo/bin/mongod", "--dbpath", dbPath, "--port", "12345")
	stdout, err := s.cmd.StdoutPipe()
	c.Assert(err, IsNil)
	outBuf := bufio.NewReader(stdout)
	c.Assert(s.cmd.Start(), IsNil)
	for {
		outLine, outErr := outBuf.ReadString('\n')
		c.Assert(outErr, IsNil)
		c.Log(outLine)
		if strings.Contains(outLine, "waiting for connections") {
			break
		}
	}
	c.Log("Started mongo test instance")
	sess, err := mgo.Dial("localhost:12345")
	c.Assert(err, IsNil)
	c.Log("Started mongo session")
	s.session = sess
	s.Db = sess.DB("test")
}

func (s *DBSuite) TearDownSuite(c *C) {
	s.session.Close()
	s.cmd.Process.Signal(syscall.SIGINT)
	s.cmd.Wait()
}

func (s *DBSuite) SetupTest(c *C) {
	s.Db.DropDatabase()
}
