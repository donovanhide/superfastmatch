package queue

import (
	"document"
	"errors"
	"log"
	"posting"
	"registry"
)

func (item *QueueItem) Execute(registry *registry.Registry, client *posting.Client) error {
	var err error
	switch item.Command {
	case "Add Document":
		err = AddDocument(item, registry, client)
	case "Delete Document":
		err = DeleteDocument(item, registry, client)
	case "Test Corpus":
		err = TestCorpus(item, registry, client)
	default:
		err = errors.New("Command does not exist!")
	}
	if err != nil {
		item.Status = "Failed"
		item.Error = err.Error()
		log.Printf("Failed Queue Item: %v", item)
	} else {
		item.Status = "Completed"
		item.Payload = []byte(nil)
		log.Printf("Completed Queue Item: %v", item)
	}
	return item.Save(registry)
}

func AddDocument(item *QueueItem, registry *registry.Registry, client *posting.Client) error {
	values, err := item.PayloadValues()
	if err != nil {
		return err
	}
	doc, err := document.NewDocument(item.Target, values)
	if err != nil {
		return err
	}
	err = doc.Save(registry)
	if err != nil {
		return err
	}
	// success := make([]bool, len(registry.PostingClients))
	err = client.CallMultiple("Posting.Add", &doc.Id)
	if err != nil {
		return err
	}
	return nil
}

func DeleteDocument(item *QueueItem, registry *registry.Registry, client *posting.Client) error {
	doc, err := document.GetDocument(item.Target, registry)
	if err != nil {
		return err
	}
	return doc.Delete(registry)
	// var success bool
	// err = doRPC("Posting.Delete", *document, &success, rw)

}

func TestCorpus(item *QueueItem, registry *registry.Registry, client *posting.Client) error {
	docs := document.BuildTestCorpus(10, 20, 500)
	for doc := <-docs; doc != nil; doc = <-docs {
		err := doc.Save(registry)
		if err != nil {
			return err
		}
		err = client.CallMultiple("Posting.Add", &doc.Id)
		if err != nil {
			return err
		}
	}
	return nil
}
