package api

import (
	"document"
	"encoding/json"
	"fmt"
	"github.com/donovanhide/eventsource"
	"labix.org/v2/mgo"
	"log"
	"net/url"
	"os"
	"queue"
	"registry"
	"strings"
)

type Feed struct {
	DocType                uint32 `bson:"_id"`
	DocId                  uint32 `json:"-"`
	Name, Url, LastEventId string
	stream                 *eventsource.Stream
}

func (f *Feed) String() string {
	return fmt.Sprintf("%s (%d,%d)", f.Name, f.DocType, f.DocId)
}

func monitor(reg *registry.Registry, feed *Feed) {
	for {
		var fields map[string]string
		form := make(url.Values)
		select {
		case event := <-feed.stream.Events:
			if err := json.Unmarshal([]byte(event.Data()), &fields); err != nil {
				log.Println(err)
				continue
			}
			feed.DocId++
			id := &document.DocumentID{
				Doctype: feed.DocType,
				Docid:   feed.DocId,
			}
			for k, v := range fields {
				form.Add(k, v)
			}
			doc, err := document.NewDocument(id, &form)
			if err != nil {
				log.Println(err)
				continue
			}
			if _, err := queue.NewQueueItem(r, "Add Document", nil, id, "", "", strings.NewReader(form.Encode())); err != nil {
				log.Println("Queueing add document:", err)
				continue
			}
			log.Printf("Received: %s %s", feed, doc.Title)
			feed.LastEventId = event.Id()
			if _, err := reg.C("feeds").UpsertId(feed.DocType, feed); err != nil {
				log.Fatalln("Updating feeds:", err)
			}
		case err := <-feed.stream.Errors:
			log.Println(err)
		}
	}
}

func MonitorFeeds(reg *registry.Registry) {
	if reg.Feeds == "" {
		return
	}
	f, err := os.Open(reg.Feeds)
	if err != nil {
		log.Fatalln("Reading feeds:", err)
	}
	defer f.Close()
	var feeds []Feed
	if err := json.NewDecoder(f).Decode(&feeds); err != nil {
		log.Fatalln("Decoding feeds:", err)
	}
	for i := range feeds {
		if err := reg.C("feeds").FindId(feeds[i].DocType).One(&feeds[i]); err != nil && err != mgo.ErrNotFound {
			log.Fatalln("Finding existing feeds:", err)
		}
		feeds[i].stream, err = eventsource.Subscribe(feeds[i].Url, feeds[i].LastEventId)
		if err == nil {
			log.Printf("Monitoring: %s", &feeds[i])
			go monitor(reg, &feeds[i])
		} else {
			log.Fatalln("Eventsource:", err)
		}
	}
}
