package testutils

import (
	. "launchpad.net/gocheck"
	"registry"
)

type DBSuite struct {
	Registry *registry.Registry
}

func (s *DBSuite) SetUpTest(c *C) {
	c.Log("Opening Registry")
	s.Registry = registry.NewRegistry([]string{"-db=test", "-api_address=localhost:9080", "-posting_addresses=localhost:9090,localhost:9091"})
	s.Registry.Open()
	c.Log("Dropping Test Database")
	s.Registry.DropDatabase()
}

func (s *DBSuite) TearDownTest(c *C) {
	c.Log("Closing Registry")
	s.Registry.Close()
}
