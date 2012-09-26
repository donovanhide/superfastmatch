package queue

import (
	"bytes"
	"compress/gzip"
	"document"
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

func NewQueueItem(registry *registry.Registry, command string,
	source *document.DocumentID, target *document.DocumentID,
	sourceRange *query.DocumentQueryParams, targetRange *query.DocumentQueryParams,
	payload io.Reader) (*QueueItem, error) {
	var buf = new(bytes.Buffer)
	w, _ := gzip.NewWriterLevel(buf, gzip.BestSpeed)
	io.Copy(w, payload)
	w.Close()
	item := QueueItem{
		Id:          bson.NewObjectId(),
		Command:     command,
		Status:      "Queued",
		Source:      source,
		Target:      target,
		SourceRange: sourceRange,
		TargetRange: targetRange,
		Payload:     buf.Bytes(),
	}
	err := item.Save(registry)
	if err != nil {
		return nil, err
	}
	return &item, err
}

func (q *QueueItem) Save(registry *registry.Registry) error {
	_, err := registry.C("queue").UpsertId(q.Id, q)
	return err
}

func (q *QueueItem) getPayload() (string, error) {
	r, err := gzip.NewReader(bytes.NewReader(q.Payload))
	if err != nil {
		return "", err
	}
	b, err := ioutil.ReadAll(r)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func (q *QueueItem) PayloadValues() (*url.Values, error) {
	payload, err := q.getPayload()
	if err != nil {
		return nil, err
	}
	values, err := url.ParseQuery(payload)
	if err != nil {
		return nil, err
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
	if err != nil {
		panic(err)
	}
	defer client.Close()
	err = client.Initialise()
	if err != nil {
		panic(err)
	}
	queue := registry.C("queue")
	for {
		select {
		case <-*registry.Queue:
			log.Println("Queue Processor Stopped")
			return
		case <-time.After(1 * time.Second):
			var item QueueItem
			iter := queue.Find(bson.M{"status": "Queued"}).Sort("_id").Iter()
			for iter.Next(&item) {
				if err := item.Execute(registry, client); err != nil {
					panic(err)
				}
			}
			if iter.Err() != nil {
				panic(iter.Err())
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
		return nil, err
	}
	stats := make(map[string]int)
	for _, item := range result {
		stats[item.Id] = item.Value
	}
	return stats, nil
}
