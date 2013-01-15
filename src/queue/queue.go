package queue

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"document"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"labix.org/v2/mgo"
	"labix.org/v2/mgo/bson"
	"log"
	"net/url"
	"posting"
	"query"
	"registry"
	"time"
)

func newQueueError(s string, err error) error {
	return errors.New(fmt.Sprint(s, err))
}

type QueueItem struct {
	Id          bson.ObjectId              `bson:"_id"`
	Command     string                     `bson:"command"`
	Source      *document.DocumentID       `bson:"source"`
	Target      *document.DocumentID       `bson:"target"`
	SourceRange *query.DocumentQueryParams `bson:"sourceRange"`
	TargetRange *query.DocumentQueryParams `bson:"targetRange"`
	Status      string                     `bson:"status"`
	Error       string                     `bson:"error"`
	Payload     []byte                     `bson:"payload"`
}

type QueueItemSlice []QueueItem

func NewQueueItem(registry *registry.Registry, command string,
	source *document.DocumentID, target *document.DocumentID,
	sourceRange *query.DocumentQueryParams, targetRange *query.DocumentQueryParams,
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
	_, err := registry.C("queue").UpsertId(q.Id, q)
	return err
}

func (q *QueueItem) UpdateStatus(registry *registry.Registry, status string) error {
	return registry.C("queue").UpdateId(q.Id, bson.M{"$set": bson.M{"status": status}})
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

func (q *QueueItem) String() string {
	var buf = new(bytes.Buffer)
	fmt.Fprint(buf, "Command: ", q.Command)
	if q.Source != nil {
		fmt.Fprint(buf, " Source: ", q.Source)
	}
	if q.Target != nil {
		fmt.Fprint(buf, " Target: ", q.Target)
	}
	if q.SourceRange != nil {
		fmt.Fprint(buf, " Source Range: ", q.SourceRange)
	}
	if q.TargetRange != nil {
		fmt.Fprint(buf, " Target Range: ", q.TargetRange)
	}
	return buf.String()
}

func Start(registry *registry.Registry) {
	log.Println("Starting Queue Processor")
	quit := make(chan bool)
	registry.Queue = &quit
	client, err := posting.NewClient(registry)
	defer client.Close()
	if err != nil {
		panic(err)
	}
	if err = client.Initialise(); err != nil {
		panic(err)
	}
	queue := registry.C("queue")
	ticker := time.NewTicker(time.Second)
	for {
		select {
		case <-*registry.Queue:
			ticker.Stop()
			log.Println("Queue Processor Stopped")
			return
		case <-ticker.C:
			start := time.Now()
			var items QueueItemSlice
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
				log.Println(err)
			}
			if len(items) > 0 {
				log.Printf("Executed %d Queue items in %.2f secs", len(items), time.Now().Sub(start).Seconds())
			}
		}
	}
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
	_, err := registry.C("queue").Find(nil).MapReduce(job, &result)
	if err != nil {
		return nil, newQueueError("Queue Map Reduce Stats:", err)
	}
	stats := make(map[string]int)
	for _, item := range result {
		stats[item.Id] = item.Value
	}
	return stats, nil
}
