package document

import (
	"bytes"
	"fmt"
	"registry"
	"sort"
	"time"
)

type DocumentArg struct {
	Id   *DocumentID
	Text string
}

type SearchResult struct {
	Success      bool          `json:"success"`
	TotalRows    int           `json:"totalRows"`
	Associations []Association `json:"documents,omitempty"`
}

type Tally struct {
	Count     uint64
	SumDeltas uint64
	Last      uint32
}

type Match struct {
	Count     uint64
	SumDeltas uint64
	Id        DocumentID
}

func (a *DocumentArg) GetDocument(registry *registry.Registry) (*Document, error) {
	if a.Id != nil {
		return GetDocument(a.Id, registry)
	}
	return BuildDocument(0, 0, "", a.Text, nil)
}

func (t *Tally) String() string {
	return fmt.Sprintf("Average Delta: %.2f Count: %d\n", float64(t.SumDeltas)/float64(t.Count), t.Count)
}

func (m *Match) String() string {
	var out bytes.Buffer
	var coverage float64
	// if m.Document != nil {
	// 	out.WriteString(fmt.Sprintf("Document: %v", m.Document))
	// 	coverage = (float64(m.Count) / float64(m.Document.Length)) * 100
	// } else {
	// 	out.WriteString(fmt.Sprintf("Document: %v", m.Id))
	// }
	out.WriteString(fmt.Sprintf(" Average Delta: %.2f Sum of Deltas: %d Count: %d Coverage: %.2f%%\n", float64(m.SumDeltas)/float64(m.Count), m.SumDeltas, m.Count, coverage))
	return out.String()
}

type MatchSlice []Match

func (m MatchSlice) Len() int      { return len(m) }
func (m MatchSlice) Swap(i, j int) { m[i], m[j] = m[j], m[i] }
func (m MatchSlice) Less(i, j int) bool {
	return float64(m[i].SumDeltas)/float64(m[i].Count) < float64(m[j].SumDeltas)/float64(m[j].Count)
}

type SearchMap map[DocumentID]*Tally

type SearchGroup []SearchMap

func (s *SearchGroup) Merge() *MatchSlice {
	merged := make(SearchMap)
	for i, _ := range *s {
		for k, v := range (*s)[i] {
			if v.SumDeltas > 0 {
				if m, ok := merged[k]; ok {
					m.Count += v.Count
					m.SumDeltas += v.SumDeltas
				} else {
					merged[k] = &Tally{
						Count:     v.Count,
						SumDeltas: v.SumDeltas,
					}
				}
			}
		}
	}
	matches := make(MatchSlice, len(merged))
	i := 0
	for k, v := range merged {
		matches[i] = Match{Id: k, Count: v.Count, SumDeltas: v.SumDeltas}
		i++
	}
	sort.Sort(matches)
	return &matches
}

func (m *MatchSlice) String() string {
	var out bytes.Buffer
	for _, v := range *m {
		out.WriteString(v.String())
	}
	return out.String()
}

func (m *MatchSlice) Fill(registry *registry.Registry, doc *Document) {
	fills := make(map[DocumentID]*Match)
	docids := make([]DocumentID, len(*m))
	for i, _ := range *m {
		docids[i] = (*m)[i].Id
		fills[(*m)[i].Id] = &(*m)[i]
	}
	searchStart := time.Now()
	for other := range GetDocuments(docids, registry) {
		start := time.Now()
		doc.AddAssociation(registry, other, false)
		fmt.Printf("Document: %v Association Time:%.2fs\n", other, time.Now().Sub(start).Seconds())
	}
	fmt.Printf("Search Time:%.2fs\n", time.Now().Sub(searchStart).Seconds())
}

func (s *SearchGroup) GetResult(registry *registry.Registry, d *DocumentArg) (*SearchResult, error) {
	doc, err := d.GetDocument(registry)
	if err != nil {
		return nil, err
	}
	s.Merge().Fill(registry, doc)
	if doc.Associations == nil {
		return &SearchResult{}, nil
	}
	return &SearchResult{
		Success:      true,
		TotalRows:    len(doc.Associations.Documents),
		Associations: doc.Associations.Documents,
	}, nil
}
