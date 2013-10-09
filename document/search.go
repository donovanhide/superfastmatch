package document

import (
	"bytes"
	"fmt"
	"github.com/donovanhide/superfastmatch/registry"
	"github.com/golang/glog"
	"math"
	"net/url"
	"sort"
	"time"
	"unicode/utf8"
)

type DocumentArg struct {
	Id          *DocumentID
	TargetRange string `schema:"target"`
	Text        string `schema:"text"`
	Limit       int    `schema:"limit"`
}

type SearchResult struct {
	Success      bool             `json:"success"`
	TotalRows    int              `json:"totalRows"`
	Associations AssociationSlice `json:"associations,omitempty"`
}

type Tally struct {
	Count           uint64
	SumDeltas       uint64
	SumSquareDeltas uint64
	Last            uint32
}

type Match struct {
	Tally
	Id DocumentID
}

func NewDocumentArg(registry *registry.Registry, values url.Values) (*DocumentArg, error) {
	d := &DocumentArg{
		Limit: 10,
	}
	decoder.Decode(d, values)
	if uint64(utf8.RuneCountInString(d.Text)) < registry.WindowSize {
		return nil, fmt.Errorf("text field less than %d unicode characters", registry.WindowSize)
	}
	return d, nil
}

func (a *DocumentArg) GetDocument(registry *registry.Registry) (*Document, error) {
	if a.Id != nil {
		return GetDocument(a.Id, registry)
	}
	return BuildDocument(0, 0, "", a.Text, nil)
}

func (t *Tally) Mean() float64 {
	return float64(t.SumDeltas) / float64(t.Count)
}

func (t *Tally) StdDev() float64 {
	mean := t.Mean()
	return math.Sqrt((float64(t.SumSquareDeltas) / float64(t.Count)) - (mean * mean))
}

func (t *Tally) Score() float64 {
	return t.StdDev() / float64(t.Count)
}

func (m *Match) String() string {
	return fmt.Sprintf("Id: %s Mean: %.2f StdDev: %.2f Sum of Deltas: %d  Sum Of Squared Deltas: %d Count: %d Score: %.2f\n", m.Id.String(), m.Mean(), m.StdDev(), m.SumDeltas, m.SumSquareDeltas, m.Count, m.Score())
}

type MatchSlice []Match

func (m MatchSlice) Len() int      { return len(m) }
func (m MatchSlice) Swap(i, j int) { m[i], m[j] = m[j], m[i] }
func (m MatchSlice) Less(i, j int) bool {
	return m[i].Score() < m[j].Score()
}

type SearchMap map[DocumentID]*Tally

type SearchGroup []SearchMap

func (s *SearchGroup) Merge(doc *DocumentArg) MatchSlice {
	merged := make(SearchMap)
	intervals := DocTypeRange(doc.TargetRange).Intervals()
	for i, _ := range *s {
		for k, v := range (*s)[i] {
			// Filter by specified doctype range
			if len(intervals) > 0 && !intervals.Contains(uint64(k.Doctype)) {
				continue
			}
			if v.Count < 8 {
				continue
			}
			if v.SumDeltas > 0 {
				if m, ok := merged[k]; ok {
					m.Count += v.Count
					m.SumDeltas += v.SumDeltas
					m.SumSquareDeltas += v.SumSquareDeltas
				} else {
					merged[k] = &Tally{
						Count:           v.Count,
						SumDeltas:       v.SumDeltas,
						SumSquareDeltas: v.SumSquareDeltas,
					}
				}
			}
		}
	}
	matches := make(MatchSlice, len(merged))
	i := 0
	for k, v := range merged {
		matches[i] = Match{Id: k, Tally: *v}
		i++
	}
	sort.Sort(matches)
	return matches
}

func (m *MatchSlice) String() string {
	var out bytes.Buffer
	for _, v := range *m {
		out.WriteString(v.String())
	}
	return out.String()
}

func (m MatchSlice) Fill(registry *registry.Registry, doc *Document) MatchSlice {
	fills := make(map[DocumentID]*Match)
	docids := make([]DocumentID, len(m))
	for i, _ := range m {
		docids[i] = m[i].Id
		fills[m[i].Id] = &m[i]
	}
	searchStart := time.Now()
	for other := range GetDocumentsById(docids, registry) {
		start := time.Now()
		doc.AddAssociation(registry, other, false)
		glog.V(2).Infof("Document: %v Association Time:%.2fs\n", other, time.Now().Sub(start).Seconds())
	}
	glog.V(2).Infof("Search Time:%.2fs\n", time.Now().Sub(searchStart).Seconds())
	return m
}

func (s *SearchGroup) GetResult(registry *registry.Registry, d *DocumentArg, save bool) (*SearchResult, error) {
	doc, err := d.GetDocument(registry)
	if err != nil {
		return nil, err
	}
	results := s.Merge(d).Fill(registry, doc)
	fmt.Println(results.String())
	if save {
		doc.Save(registry)
	}
	if d.Limit < len(results) {
		results = results[:d.Limit]
	}
	return &SearchResult{
		Success:      true,
		TotalRows:    len(doc.Associations),
		Associations: doc.Associations,
	}, nil
}
