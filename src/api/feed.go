package api

import (
	"github.com/donovanhide/eventsource"
	"log"
	"registry"
)

func MonitorFeeds(reg *registry.Registry) {
	feeds := make([]*eventsource.Stream, 0)
	for _, feed := range reg.FeedConfigs {
		err := reg.C("feeds").FindId(feed.Url).One(&feed)
		log.Println(feed)
		f, err := eventsource.Subscribe(feed.Url, feed.LastEventId)
		if err == nil {
			feeds = append(feeds, f)
		} else {
			log.Println(err)
		}
	}
}
