package registry

import (
	"flag"
	"fmt"
	"labix.org/v2/mgo"
	"log"
	"net"
	"sync"
)

type flags struct {
	WindowSize       windowSize
	HashWidth        hashWidth
	GroupSize        groupSize
	Db               string
	ApiAddress       string
	MongoUrl         string
	PostingAddresses addresses
	Feeds            feeds
	InitialQuery     query
}

type FeedConfig struct {
	Url         string
	LastEventId string
	lock        sync.Mutex
}

type PostingConfig struct {
	Address      string
	HashWidth    uint64
	WindowSize   uint64
	Offset       uint64
	Size         uint64
	GroupSize    uint64
	InitialQuery string
}

type Registry struct {
	Mode             string
	HashWidth        uint64
	WindowSize       uint64
	Routines         sync.WaitGroup
	Queue            chan bool
	ApiListener      net.Listener
	ApiAddress       string
	PostingListeners []net.Listener
	PostingConfigs   []PostingConfig
	FeedConfigs      []FeedConfig
	session          *mgo.Session
	flags            *flags
}

func parseFlags(args []string) *flags {
	f := flags{
		WindowSize:       30,
		HashWidth:        24,
		GroupSize:        24,
		PostingAddresses: []string{"127.0.0.1:8090", "127.0.0.1:8091"},
	}
	flags := flag.NewFlagSet("flags", flag.ExitOnError)
	flags.Usage = func() {
		flags.PrintDefaults()
		fmt.Println("Follow superfastmatch with either a mode or a client command to alter behaviour")
		fmt.Println("Modes: api queue posting")
		fmt.Println("Client commands: add delete search associate")
	}
	flags.Var(&f.WindowSize, "window_size", "Specify the Window Size to use for hashing.")
	flags.Var(&f.HashWidth, "hash_width", "Specify the number of bits to use for hashing.")
	flags.Var(&f.GroupSize, "group_size", "Specify the block size of the sparsetable.")
	flags.Var(&f.InitialQuery, "initial_query", "Specify the range of doctypes to load initially. Blank string equals all documents.")
	flags.StringVar(&f.ApiAddress, "api_address", "127.0.0.1:8080", "Address for API to listen on.")
	flags.StringVar(&f.MongoUrl, "mongo_url", "127.0.0.1:27017/superfastmatch", "Url to connect to MongoDB with.")
	flags.Var(&f.PostingAddresses, "posting_addresses", "Comma-separated list of addresses for Posting Servers.")
	flags.Var(&f.Feeds, "feeds", "Comma-separated list of addresses for eventsource feeds.")
	flags.Parse(args)
	return &f
}

func parseMode(args []string) (string, []string) {
	if len(args) == 1 {
		return "standalone", []string{}
	}
	switch args[1] {
	case "api":
		return "api", args[2:]
	case "posting":
		return "posting", args[2:]
	case "queue":
		return "queue", args[2:]
	case "add", "delete", "associate", "switch", "search":
		return "client", args[1:]
	}
	return "standalone", args[1:]
}

func (r *Registry) Open() {
	var err error
	r.HashWidth = uint64(r.flags.HashWidth)
	r.WindowSize = uint64(r.flags.WindowSize)
	r.ApiAddress = r.flags.ApiAddress
	if r.session, err = mgo.Dial(r.flags.MongoUrl); err != nil {
		log.Fatalf("Error connecting to mongo instance: %s", err)
	}
	if r.Mode == "posting" || r.Mode == "standalone" {
		r.PostingListeners = make([]net.Listener, len(r.flags.PostingAddresses))
		for i, postingAddress := range r.flags.PostingAddresses {
			r.PostingListeners[i], err = net.Listen("tcp", postingAddress)
			checkErr(err)
		}
	}
	if r.Mode == "api" || r.Mode == "standalone" {
		r.ApiListener, err = net.Listen("tcp", r.flags.ApiAddress)
		checkErr(err)
		size := (uint64(1) << r.HashWidth) / uint64(len(r.flags.PostingAddresses))
		for i, postingAddress := range r.flags.PostingAddresses {
			p := PostingConfig{
				HashWidth:    uint64(r.flags.HashWidth),
				WindowSize:   uint64(r.flags.WindowSize),
				Size:         size,
				Offset:       size * uint64(i),
				GroupSize:    uint64(r.flags.GroupSize),
				InitialQuery: r.flags.InitialQuery.String(),
				Address:      postingAddress,
			}
			r.PostingConfigs = append(r.PostingConfigs, p)
		}
		for _, feed := range r.flags.Feeds {
			r.FeedConfigs = append(r.FeedConfigs, FeedConfig{
				Url: feed,
			})
		}
	}
}

func (r *Registry) Close() {
	if r.Mode == "standalone" || r.Mode == "api" {
		checkErr(r.ApiListener.Close())
	}
	if r.Mode == "standalone" || r.Mode == "queue" {
		if r.Queue != nil {
			r.Queue <- true
		}
	}
	if r.Mode == "standalone" || r.Mode == "posting" {
		for i, _ := range r.flags.PostingAddresses {
			checkErr(r.PostingListeners[i].Close())
		}
	}
	r.Routines.Wait()
	r.session.Close()
}

func NewRegistry(args []string) *Registry {
	r := new(Registry)
	r.Mode, args = parseMode(args)
	r.flags = parseFlags(args)
	return r
}

func (r *Registry) DropDatabase() error {
	return r.session.DB("").DropDatabase()
}

func (r *Registry) C(name string) *mgo.Collection {
	return r.session.Clone().DB("").C(name)
}
