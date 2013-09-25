package queue

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"errors"
	"fmt"
	"github.com/donovanhide/superfastmatch/document"
	"github.com/donovanhide/superfastmatch/posting"
	"github.com/donovanhide/superfastmatch/registry"
	"github.com/golang/glog"
	"io"
	"io/ioutil"
	"labix.org/v2/mgo"
	"labix.org/v2/mgo/bson"
	"net/url"
	"time"
)

func newQueueError(s string, err error) error {
	return errors.New(fmt.Sprint(s, err))
}

type QueueResult struct {
	Success   bool           `json:"success"`
	Rows      QueueItemSlice `json:"rows"`
	TotalRows int            `json:"totalRows"`
}

type QueueItem struct {
	Id          bson.ObjectId        `bson:"_id" json:"id"`
	Command     string               `bson:"command" json:"command"`
	Source      *document.DocumentID `bson:"source" json:"source"`
	Target      *document.DocumentID `bson:"target" json:"target"`
	SourceRange string               `bson:"sourceRange" json:"sourceRange"`
	TargetRange string               `bson:"targetRange" json:"targetRange"`
	Status      string               `bson:"status" json:"status"`
	Error       string               `bson:"error" json:"error"`
	Payload     []byte               `bson:"payload" json:"-"`
}

type QueueItemSlice []QueueItem

func NewQueueItem(registry *registry.Registry, command string,
	source *document.DocumentID, target *document.DocumentID,
	sourceRange string, targetRange string,
	payload io.Reader) (*QueueItem, error) {
	buf := new(bytes.Buffer)
	w, _ := gzip.NewWriterLevel(buf, gzip.BestSpeed)
	if _, err := io.Copy(w, payload); err != nil {
		return nil, newQueueError("Queue Item gzip copy:", err)
	}
	if err := w.Close(); err != nil {
		return nil, newQueueError("Queue Item gzip close:", err)
	}
	item := &QueueItem{
		Id:          bson.NewObjectId(),
		Command:     command,
		Status:      "Queued",
		Source:      source,
		Target:      target,
		SourceRange: sourceRange,
		TargetRange: targetRange,
		Payload:     buf.Bytes(),
	}
	if err := item.Save(registry); err != nil {
		return nil, newQueueError("Queue Item save:", err)
	}
	return item, nil
}

func (q *QueueItem) Save(registry *registry.Registry) error {
	db := registry.DB()
	defer db.Session.Close()
	_, err := db.C("queue").UpsertId(q.Id, q)
	return err
}

func (q *QueueItem) UpdateStatus(registry *registry.Registry, status string) error {
	db := registry.DB()
	defer db.Session.Close()
	return db.C("queue").UpdateId(q.Id, bson.M{"$set": bson.M{"status": status}})
}

func (q *QueueItem) getPayload() (string, error) {
	buf := bufio.NewReader(bytes.NewBuffer(q.Payload))
	r, err := gzip.NewReader(buf)
	if err != nil {
		return "", newQueueError("Queue Item Get Payload:", err)
	}
	b, err := ioutil.ReadAll(r)
	if err != nil {
		return "", newQueueError("Queue Item Read Payload:", err)
	}
	return string(b), nil
}

func (q *QueueItem) PayloadValues() (*url.Values, error) {
	payload, err := q.getPayload()
	if err != nil {
		return nil, newQueueError("Queue Item Get Payload Values:", err)
	}
	values, err := url.ParseQuery(payload)
	if err != nil {
		return nil, newQueueError("Queue Item Parse Payload Values:", err)
	}
	return &values, nil
}

func (q *QueueItem) Location(registry *registry.Registry) string {
	switch q.Command {
	case "Add Document":
		return fmt.Sprintf("http://%s/document/%d/%d/", registry.ApiAddress, q.Target.Doctype, q.Target.Docid)
	}
	return ""
}

func (q *QueueItem) String() string {
	var buf = new(bytes.Buffer)
	fmt.Fprint(buf, "Command: ", q.Command)
	if q.Source != nil {
		fmt.Fprint(buf, " Source: ", q.Source)
	}
	if q.Target != nil {
		fmt.Fprint(buf, " Target: ", q.Target)
	}
	if q.SourceRange != "" {
		fmt.Fprint(buf, " Source Range: ", q.SourceRange)
	}
	if q.TargetRange != "" {
		fmt.Fprint(buf, " Target Range: ", q.TargetRange)
	}
	if q.Error != "" {
		fmt.Fprint(buf, " Error: ", q.Error)
	}
	return buf.String()
}

func Start(registry *registry.Registry) {
	glog.Infoln("Starting Queue Processor")
	registry.Queue = make(chan bool)
	client, err := posting.NewClient(registry)
	if err != nil {
		panic(err)
	}
	defer client.Close()
	if err = client.Initialise(); err != nil {
		panic(err)
	}
	db := registry.DB()
	defer db.Session.Close()
	queue := db.C("queue")
	registry.Routines.Add(1)
	var items QueueItemSlice
	for {
		start := time.Now()
		if err := queue.Find(bson.M{"status": "Queued"}).Sort("_id").Limit(10).All(&items); err != nil {
			panic(err)
		}
		for i, item := range items {
			if item.Command != items[0].Command {
				items = items[:i]
				break
			}
		}
		if err := items.Execute(registry, client); err != nil {
			glog.Errorln(err)
		}
		if len(items) > 0 {
			glog.Infof("Executed %d Queue items in %.2f secs", len(items), time.Now().Sub(start).Seconds())
			continue
		}
		select {
		case <-registry.Queue:
			glog.Infoln("Queue Processor Stopped")
			registry.Routines.Done()
			return
		default:
			time.Sleep(time.Second)
		}
	}
}

func GetQueue(values url.Values, registry *registry.Registry) (*QueueResult, error) {
	var items []QueueItem
	db := registry.DB()
	defer db.Session.Close()
	if err := db.C("queue").Find(nil).Select(bson.M{"payload": 0}).Sort("_id").All(&items); err != nil {
		return nil, fmt.Errorf("Queue item not found: %s", err)
	}
	return &QueueResult{
		Rows:      items,
		TotalRows: len(items),
		Success:   true,
	}, nil
}

func getQueueItem(id bson.ObjectId, registry *registry.Registry) (*QueueItem, error) {
	var item QueueItem
	db := registry.DB()
	defer db.Session.Close()
	if err := db.C("queue").FindId(id).Select(bson.M{"payload": 0}).One(&item); err != nil {
		return nil, fmt.Errorf("Queue item not found: %s", err)
	}
	return &item, nil
}

func GetQueueItem(values url.Values, registry *registry.Registry) (*QueueItem, error) {
	if id := values.Get("id"); id != "" {
		return getQueueItem(bson.ObjectIdHex(id), registry)
	}
	return nil, fmt.Errorf("Missing queue id")
}

func Stats(registry *registry.Registry) (map[string]int, error) {
	job := &mgo.MapReduce{
		Map:    "function() { emit(this.status, 1) }",
		Reduce: "function(key, values) { return Array.sum(values) }",
	}
	var result []struct {
		Id    string "_id"
		Value int
	}
	db := registry.DB()
	defer db.Session.Close()
	_, err := db.C("queue").Find(nil).MapReduce(job, &result)
	if err != nil {
		return nil, newQueueError("Queue Map Reduce Stats:", err)
	}
	stats := make(map[string]int)
	for _, item := range result {
		stats[item.Id] = item.Value
	}
	return stats, nil
}
