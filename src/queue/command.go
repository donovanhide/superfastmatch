package queue

import (
	"document"
	"errors"
	"fmt"
	"log"
	"posting"
	"registry"
)

type QueueItemRun struct {
	item *QueueItem
	err  error
}

type commandFunc func(item *QueueItem, registry *registry.Registry, client *posting.Client, c chan *QueueItemRun)

var commandMap = map[string]commandFunc{
	"Add Document":    AddDocument,
	"Delete Document": DeleteDocument,
	"Test Corpus":     TestCorpus,
}

func runFailure(item *QueueItem, s string, err error) *QueueItemRun {
	return &QueueItemRun{item, errors.New(fmt.Sprintf("%s: %s", s, err))}
}

func runSuccess(item *QueueItem) *QueueItemRun {
	return &QueueItemRun{item, nil}
}

func (items QueueItemSlice) Execute(registry *registry.Registry, client *posting.Client) error {
	c := make(chan *QueueItemRun, len(items))
	for i := range items {
		item := &items[i]
		f, ok := commandMap[item.Command]
		if !ok {
			return errors.New("Command does not exist!")
		}
		if err := item.UpdateStatus(registry, "Started"); err != nil {
			return err
		}
		go f(item, registry, client, c)
	}
	for i := 0; i < len(items); i++ {
		run := <-c
		if run.err != nil {
			run.item.Status = "Failed"
			run.item.Error = run.err.Error()
			log.Printf("Failed Queue Item: %v Error: %s", run.item, run.err)
		} else {
			run.item.Status = "Completed"
			run.item.Payload = []byte(nil)
		}
		if err := run.item.Save(registry); err != nil {
			return err
		}
	}
	return nil
}

func AddDocument(item *QueueItem, registry *registry.Registry, client *posting.Client, c chan *QueueItemRun) {
	values, err := item.PayloadValues()
	if err != nil {
		c <- runFailure(item, "Get Payload", err)
		return
	}
	doc, err := document.NewDocument(item.Target, values)
	if err != nil {
		c <- runFailure(item, "New Document", err)
		return
	}
	if err = doc.Save(registry); err != nil {
		c <- runFailure(item, "Save Document", err)
		return
	}
	if err = client.CallMultiple("Posting.Add", &document.DocumentArg{Id: &doc.Id}); err != nil {
		c <- runFailure(item, "RPC Call", err)
		return
	}
	c <- runSuccess(item)
}

func DeleteDocument(item *QueueItem, registry *registry.Registry, client *posting.Client, c chan *QueueItemRun) {
	doc, err := document.GetDocument(item.Target, registry)
	if err != nil {
		c <- runFailure(item, "Get Document", err)
		return
	}
	if err = client.CallMultiple("Posting.Delete", &document.DocumentArg{Id: &doc.Id}); err != nil {
		c <- runFailure(item, "RPC Call", err)
		return
	}
	if err = doc.Delete(registry); err != nil {
		c <- runFailure(item, "Delete Document", err)
		return
	}
	c <- runSuccess(item)
}

func TestCorpus(item *QueueItem, registry *registry.Registry, client *posting.Client, c chan *QueueItemRun) {
	docs := document.BuildTestCorpus(10, 20, 500)
	for doc := <-docs; doc != nil; doc = <-docs {
		if err := doc.Save(registry); err != nil {
			c <- runFailure(item, "Save Document", err)
			return
		}
		if err := client.CallMultiple("Posting.Add", &document.DocumentArg{Id: &doc.Id}); err != nil {
			c <- runFailure(item, "RPC Call", err)
			return
		}
	}
	c <- runSuccess(item)
}
