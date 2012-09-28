package registry

import (
	"flag"
	"labix.org/v2/mgo"
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
	InitialQuery     query
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
	Queue            *chan bool
	ApiListener      net.Listener
	PostingListeners []net.Listener
	PostingConfigs   []PostingConfig
	db               string
	session          *mgo.Session
	flags            *flags
}

func parseFlags(args []string) *flags {
	f := flags{
		WindowSize:       16,
		HashWidth:        24,
		GroupSize:        24,
		PostingAddresses: []string{"127.0.0.1:8090", "127.0.0.1:8091"},
	}
	flags := flag.NewFlagSet("flags", flag.ExitOnError)
	flags.Var(&f.WindowSize, "window_size", "Specify the Window Size to use for hashing.")
	flags.Var(&f.HashWidth, "hash_width", "Specify the number of bits to use for hashing.")
	flags.Var(&f.GroupSize, "group_size", "Specify the block size of the sparsetable.")
	flags.Var(&f.InitialQuery, "initial_query", "Specify the range of doctypes to load initially. Blank string equals all documents.")
	flags.StringVar(&f.Db, "db", "superfastmatch", "Name of mongo database.")
	flags.StringVar(&f.ApiAddress, "api_address", "127.0.0.1:8080", "Address for API to listen on.")
	flags.StringVar(&f.MongoUrl, "mongo_url", "127.0.0.1:27017", "Url to connect to MongoDB with.")
	flags.Var(&f.PostingAddresses, "posting_addresses", "Comma-separated list of addresses for Posting Servers.")
	flags.Parse(args)
	return &f
}

func parseMode(args []string) (string, []string) {
	switch {
	case len(args) == 1:
		return "standalone", []string{}
	case args[1] == "api":
		return "api", args[2:]
	case args[1] == "posting":
		return "posting", args[2:]
	}
	return "standalone", args[1:]
}

func (r *Registry) Open() {
	var err error
	r.HashWidth = uint64(r.flags.HashWidth)
	r.WindowSize = uint64(r.flags.WindowSize)
	r.db = r.flags.Db
	r.session, err = mgo.Dial(r.flags.MongoUrl)
	checkErr(err)
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
	}
}

func (r *Registry) Close() {
	if r.Mode == "standalone" || r.Mode == "api" {
		checkErr(r.ApiListener.Close())
		if r.Queue != nil {
			*r.Queue <- true
			r.Queue = nil
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
	return r.session.DB(r.db).DropDatabase()
}

func (r *Registry) C(name string) *mgo.Collection {
	return r.session.Clone().DB(r.db).C(name)
}
