package queue

import (
	"document"
	"errors"
	"labix.org/v2/mgo"
	"log"
)

func (item *QueueItem) Execute(db *mgo.Database) error {
	var err error
	switch item.Command {
	case "Add Document":
		err = AddDocument(item, db)
	case "Delete Document":
		err = DeleteDocument(item, db)
	case "Test Corpus":
		err = TestCorpus(item, db)
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
	return item.Save(db)
}

func AddDocument(item *QueueItem, Db *mgo.Database) error {
	values, err := item.PayloadValues()
	if err != nil {
		return err
	}
	doc, err := document.NewDocument(item.Target, values)
	if err != nil {
		return err
	}
	return doc.Save(Db)
	// var success bool
	// err = doRPC("Posting.Delete", *document, &success, rw)

}

func DeleteDocument(item *QueueItem, db *mgo.Database) error {
	doc, err := document.GetDocument(item.Target, db)
	if err != nil {
		return err
	}
	return doc.Delete(db)
	// var success bool
	// err = doRPC("Posting.Delete", *document, &success, rw)

}

func TestCorpus(item *QueueItem, db *mgo.Database) error {
	return document.BuildTestCorpus(db, 100, 100, 5000)
}
