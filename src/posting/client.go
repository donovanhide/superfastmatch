package posting

import (
	"code.google.com/p/gorilla/schema"
	"net/rpc"
	"net/url"
	"registry"
)

var decoder = schema.NewDecoder()

type Client struct {
	clients []*rpc.Client
	configs []registry.PostingConfig
}

type QueryParams struct {
	Start uint64 `schema:"start"`
	Limit int    `schema:"limit"`
}

type Doctypes struct {
	Doctype uint64   `json:"doctype"`
	Docids  []uint64 `json:"docids"`
	Deltas  []uint64 `json:"deltas"`
}

type Hash struct {
	Hash     uint64   `json:"hash"`
	Bytes    int      `json:"bytes"`
	Doctypes Doctypes `json:"doctypes"`
}

type ListResult struct {
	Rows      []Hash `json:"rows"`
	TotalRows uint64 `json:"totalRows"`
}

func NewClient(registry *registry.Registry) (*Client, error) {
	p := &Client{
		configs: registry.PostingConfigs,
	}
	p.clients = make([]*rpc.Client, len(registry.PostingConfigs))
	var err error
	for i, _ := range p.configs {
		p.clients[i], err = rpc.Dial("tcp", p.configs[i].Address)
		if err != nil {
			return nil, err
		}
	}
	return p, nil
}

func (p *Client) Initialise() error {
	done := make(chan *rpc.Call, len(p.configs))
	for i, _ := range p.configs {
		p.clients[i].Go("Posting.Init", p.configs[i], nil, done)
	}
	for _, _ = range p.configs {
		replyCall := <-done
		if replyCall.Error != nil {
			return replyCall.Error
		}
	}
	return nil
}

func (p *Client) Close() {
	for _, c := range p.clients {
		c.Close()
	}
}

// Don't care about the replies, just check the error
func (p *Client) CallMultiple(service string, args interface{}) error {
	done := make(chan *rpc.Call, len(p.clients))
	for i, _ := range p.clients {
		p.clients[i].Go(service, args, nil, done)
	}
	for _, _ = range p.configs {
		replyCall := <-done
		if replyCall.Error != nil {
			return replyCall.Error
		}
	}
	return nil
}

func (p *Client) GetRows(values *url.Values) (*ListResult, error) {
	q := &QueryParams{
		Start: 0,
		Limit: 100,
	}
	r := new(ListResult)
	decoder.Decode(q, *values)
	for i, _ := range p.clients {
		if p.configs[i].Offset+p.configs[i].Size > q.Start {
			err := p.clients[i].Call("Posting.List", q, r)
			if err != nil {
				return nil, err
			}
		}
	}
	return r, nil
}
