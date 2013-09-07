package api

import (
	"encoding/json"
	"fmt"
	"github.com/donovanhide/eventsource"
	"github.com/donovanhide/superfastmatch/document"
	"github.com/donovanhide/superfastmatch/queue"
	"github.com/donovanhide/superfastmatch/registry"
	"github.com/golang/glog"
	"labix.org/v2/mgo"
	"net/url"
	"os"
	"os/signal"
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
	db := reg.DB()
	db.Session.SetSyncTimeout(0)
	defer db.Session.Close()
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt)
	for {
		var fields map[string]interface{}
		form := make(url.Values)
		select {
		case <-sig:
			return
		case event := <-feed.stream.Events:
			if err := json.Unmarshal([]byte(event.Data()), &fields); err != nil {
				glog.Errorln(err)
				continue
			}
			feed.DocId++
			id := &document.DocumentID{
				Doctype: feed.DocType,
				Docid:   feed.DocId,
			}
			for k, v := range fields {
				switch v.(type) {
				case string:
					form.Add(k, v.(string))
				case []string:
					form.Add(k, strings.Join(v.([]string), "|"))
				}
			}
			doc, err := document.NewDocument(id, &form)
			if err != nil {
				glog.Errorln(err)
				continue
			}
			if _, err := queue.NewQueueItem(r, "Add Document", nil, id, "", "", strings.NewReader(form.Encode())); err != nil {
				glog.Infoln("Queueing add document:", err)
				continue
			}
			glog.Infof("Received: %s %s", feed, doc.Title)
			feed.LastEventId = event.Id()
			if _, err := db.C("feeds").UpsertId(feed.DocType, feed); err != nil {
				glog.Fatalln("Updating feeds:", err)
			}
		case err := <-feed.stream.Errors:
			glog.Errorln(err)
		}
	}
}

func MonitorFeeds(reg *registry.Registry) {
	if reg.Feeds == "" {
		return
	}
	f, err := os.Open(reg.Feeds)
	if err != nil {
		glog.Fatalln("Reading feeds:", err)
	}
	defer f.Close()
	var feeds []Feed
	if err := json.NewDecoder(f).Decode(&feeds); err != nil {
		glog.Fatalln("Decoding feeds:", err)
	}
	db := reg.DB()
	defer db.Session.Close()
	for i := range feeds {
		if err := db.C("feeds").FindId(feeds[i].DocType).One(&feeds[i]); err != nil && err != mgo.ErrNotFound {
			glog.Fatalln("Finding existing feeds:", err)
		}
		feeds[i].stream, err = eventsource.Subscribe(feeds[i].Url, feeds[i].LastEventId)
		if err == nil {
			glog.Infof("Monitoring: %s", &feeds[i])
			go monitor(reg, &feeds[i])
		} else {
			glog.Fatalln("Eventsource:", err)
		}
	}
}
