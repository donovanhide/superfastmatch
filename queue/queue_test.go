package queue

import (
	"github.com/donovanhide/superfastmatch/document"
	"github.com/donovanhide/superfastmatch/posting"
	"github.com/donovanhide/superfastmatch/testutils"
	. "launchpad.net/gocheck"
	"strings"
	"testing"
	"time"
)

func Test(t *testing.T) { TestingT(t) }

type QuerySuite struct {
	testutils.DBSuite
}

var _ = Suite(&QuerySuite{})

func waitForItem(item *QueueItem, s *QuerySuite) error {
	var err error
	for {
		if item, err = getQueueItem(item.Id, s.Registry); err != nil {
			return err
		}
		if item.Status == "Completed" {
			return nil
		}
		time.Sleep(time.Second)
	}
	return nil
}

func (s *QuerySuite) TestQueue(c *C) {
	go posting.Serve(s.Registry)
	go Start(s.Registry)
	var item *QueueItem
	var err error
	for i := uint32(1); i <= 20; i++ {
		target := document.DocumentID{Doctype: 1, Docid: i}
		item, err = NewQueueItem(s.Registry, "Add Document", nil, &target, "", "", strings.NewReader("title=Payload&text=PayloadWithsometextlongerthanwindowsize"))
		c.Check(item, NotNil)
		c.Check(err, IsNil)
	}
	waitForItem(item, s)
	stats, err := Stats(s.Registry)
	c.Check(err, IsNil)
	c.Check(stats["Completed"], Equals, 20)
	c.Check(stats["Queued"], Equals, 0)
	c.Check(stats["Failed"], Equals, 0)
	db := s.Registry.DB()
	defer db.Session.Close()
	count, err := db.C("documents").Count()
	c.Check(err, IsNil)
	c.Check(count, Equals, 20)
	for i := uint32(1); i <= 20; i++ {
		target := document.DocumentID{Doctype: 1, Docid: i}
		item, err = NewQueueItem(s.Registry, "Delete Document", nil, &target, "", "", strings.NewReader(""))
		c.Check(item, NotNil)
		c.Check(err, IsNil)
	}
	waitForItem(item, s)
	stats, err = Stats(s.Registry)
	c.Check(err, IsNil)
	c.Check(stats["Completed"], Equals, 40)
	c.Check(stats["Queued"], Equals, 0)
	c.Check(stats["Failed"], Equals, 0)
	count, err = db.C("documents").Count()
	c.Check(err, IsNil)
	c.Check(count, Equals, 0)
}

func (s *QuerySuite) TestPayload(c *C) {
	go Start(s.Registry)
	go posting.Serve(s.Registry)
	item, err := NewQueueItem(s.Registry, "test", nil, nil, "", "", strings.NewReader("I am the payload"))
	c.Check(err, IsNil)
	var q QueueItem
	db := s.Registry.DB()
	defer db.Session.Close()
	db.C("queue").FindId(item.Id).One(&q)
	c.Check(q.Payload, NotNil)
	p, err := q.getPayload()
	c.Check(p, Equals, "I am the payload")
	c.Check(err, IsNil)
}

func (s *QuerySuite) TestAssociate(c *C) {
	go Start(s.Registry)
	go posting.Serve(s.Registry)
	item, err := NewQueueItem(s.Registry, "Test Corpus", nil, nil, "", "", strings.NewReader(""))
	c.Check(err, IsNil)
	c.Check(waitForItem(item, s), IsNil)
	item, err = NewQueueItem(s.Registry, "Associate Document", nil, nil, "1", "2-10", strings.NewReader(""))
	c.Check(err, IsNil)
	c.Check(waitForItem(item, s), IsNil)
}
