package posting

type DocumentID struct {
	doctype uint32
	docid   uint32
}

type Document struct {
	id     DocumentID
	hashes uint32
}

type Result struct {
	id DocumentID
	//	results map[DocumentID][]uint32
}

type Posting uint64

func (p *Posting) Add(document *Document, reply *bool) error {
	return nil
}

func (p *Posting) Delete(document *Document, reply *bool) error {
	return nil
}

func (p *Posting) Search(document *Document, result *Result) error {
	return nil
}
