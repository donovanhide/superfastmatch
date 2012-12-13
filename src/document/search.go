package document

import (
	"fmt"
	"registry"
	"sort"
)

type SearchResult struct {
	Success   bool          `json:"success"`
	TotalRows uint64        `json:"totalRows"`
	Documents []DocumentRow `json:"documents"`
}

type DocumentRow struct {
	Document
	Fragments     []uint32 `json:"fragments"`
	FragmentCount uint32   `json:"fragment_count"`
}

type Tally struct {
	Last      uint32
	Count     uint64
	SumDeltas uint64
}

func (t *Tally) String() string {
	return fmt.Sprintf("Average Delta: %.2f Count: %d\n", float64(t.SumDeltas)/float64(t.Count), t.Count)
}

type Match struct {
	Id        DocumentID
	Count     uint64
	SumDeltas uint64
}

func (m *Match) String() string {
	return fmt.Sprintf("Document: %v Average Delta: %.2f Sum of Deltas: %d Count: %d", m.Id, float64(m.SumDeltas)/float64(m.Count), m.SumDeltas, m.Count)
}

type Matches []Match

func (m Matches) Len() int      { return len(m) }
func (m Matches) Swap(i, j int) { m[i], m[j] = m[j], m[i] }
func (m Matches) Less(i, j int) bool {
	return float64(m[i].SumDeltas)/float64(m[i].Count) < float64(m[j].SumDeltas)/float64(m[j].Count)
}

type SearchMap map[DocumentID]*Tally

type SearchGroup []SearchMap

func (s *SearchGroup) Merge() Matches {
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
	matches := make(Matches, len(merged))
	i := 0
	for k, v := range merged {
		matches[i] = Match{Id: k, Count: v.Count, SumDeltas: v.SumDeltas}
		i++
	}
	sort.Sort(matches)
	return matches
}

func (s *SearchGroup) GetResult(registry *registry.Registry) *SearchResult {
	matches := s.Merge()
	docids := make([]DocumentID, len(matches))
	for i, m := range matches {
		docids[i] = m.Id
		fmt.Println(m.String())
	}
	ch := GetDocuments(docids, registry)
	for doc := range ch {
		fmt.Println(doc.Title)
	}
	return nil
}
