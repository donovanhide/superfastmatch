package queue

import (
	"document"
	. "launchpad.net/gocheck"
	"strings"
	"testing"
	"testutils"
	"time"
)

func Test(t *testing.T) { TestingT(t) }

type QuerySuite struct {
	testutils.DBSuite
}

var _ = Suite(&QuerySuite{})

func (s *QuerySuite) TestQueue(c *C) {
	Start(s.Db())
	defer Stop()
	for i := uint32(1); i <= 20; i++ {
		target := document.DocumentID{Doctype: 1, Docid: i}
		item, err := NewQueueItem(s.Db(), "Add Document", nil, &target, nil, nil, strings.NewReader("title=Payload&text=Payload"))
		c.Check(item, NotNil)
		c.Check(err, IsNil)
	}
	time.Sleep(2 * time.Second)
	stats, err := Stats(s.Db())
	c.Check(err, IsNil)
	c.Check(stats["Completed"], Equals, 20)
	c.Check(stats["Queued"], Equals, 0)
	c.Check(stats["Failed"], Equals, 0)
	count, err := s.Db().C("documents").Count()
	c.Check(err, IsNil)
	c.Check(count, Equals, 20)
	for i := uint32(1); i <= 20; i++ {
		target := document.DocumentID{Doctype: 1, Docid: i}
		item, err := NewQueueItem(s.Db(), "Delete Document", nil, &target, nil, nil, strings.NewReader(""))
		c.Check(item, NotNil)
		c.Check(err, IsNil)
	}
	time.Sleep(2 * time.Second)
	stats, err = Stats(s.Db())
	c.Check(err, IsNil)
	c.Check(stats["Completed"], Equals, 40)
	c.Check(stats["Queued"], Equals, 0)
	c.Check(stats["Failed"], Equals, 0)
	count, err = s.Db().C("documents").Count()
	c.Check(err, IsNil)
	c.Check(count, Equals, 0)
}

func (s *QuerySuite) TestPayload(c *C) {
	_, err := NewQueueItem(s.Db(), "test", nil, nil, nil, nil, strings.NewReader("I am the payload"))
	c.Check(err, IsNil)
	var q QueueItem
	s.Db().C("queue").Find(nil).One(&q)
	c.Check(q.Payload, NotNil)
	p, err := q.getPayload()
	c.Check(p, Equals, "I am the payload")
	c.Check(err, IsNil)
}
