package registry

import (
	. "launchpad.net/gocheck"
	"testing"
)

func Test(t *testing.T) { TestingT(t) }

type RegistrySuite struct{}

var _ = Suite(&RegistrySuite{})

func (s *RegistrySuite) TestNewRegistry(c *C) {
	defaults := NewRegistry([]string{""})
	defaults.Open()
	c.Check(defaults.session, NotNil)
	c.Check(defaults.HashWidth, Equals, uint64(24))
	c.Check(defaults.WindowSize, Equals, uint64(30))
	defaults.Close()
	r := NewRegistry([]string{"api", "-hash_width=32", "-window_size=40"})
	r.Open()
	c.Check(r.session, NotNil)
	c.Check(r.HashWidth, Equals, uint64(32))
	c.Check(r.WindowSize, Equals, uint64(40))
	r.Close()
}
