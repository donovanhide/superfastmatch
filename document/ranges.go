package document

import (
	"labix.org/v2/mgo/bson"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

type Interval struct {
	start, end uint64
}

type IntervalSlice []Interval

func (s IntervalSlice) Len() int           { return len(s) }
func (s IntervalSlice) Less(i, j int) bool { return s[i].start < s[j].start }
func (s IntervalSlice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

type DocTypeRange string

func (s IntervalSlice) Contains(value uint64) bool {
	i := sort.Search(len(s), func(j int) bool {
		return s[j].end >= value
	})
	if i < len(s) && s[i].start <= value && s[i].end >= value {
		return true
	}
	return false
}

var docTypeRangeRegex = regexp.MustCompile(`^(\d+(-\d+)?(:\d+(-\d+)?)*)*$`)

func (r DocTypeRange) Valid() bool {
	return docTypeRangeRegex.MatchString(string(r))
}

func (r DocTypeRange) Intervals() IntervalSlice {
	if len(r) == 0 || !r.Valid() {
		return IntervalSlice{}
	}
	sections := strings.Split(string(r), ":")
	intervals := make(IntervalSlice, len(sections))
	for i, f := range sections {
		g := strings.Split(f, "-")
		start, _ := strconv.ParseUint(g[0], 10, 32)
		if len(g) == 2 {
			end, _ := strconv.ParseUint(g[1], 10, 32)
			if start > end {
				intervals[i] = Interval{end, start}
			} else {
				intervals[i] = Interval{start, end}
			}

		} else {
			intervals[i] = Interval{start, start}
		}
	}
	sort.Sort(intervals)
	return intervals
}

func (r DocTypeRange) Parse() bson.M {
	if len(r) == 0 {
		return bson.M{}
	}
	intervals := r.Intervals()
	filter := make([]bson.M, len(intervals))
	for i, interval := range intervals {
		if interval.start != interval.end {
			filter[i] = bson.M{"_id.doctype": bson.M{"$gte": interval.start, "$lte": interval.end}}
		} else {
			filter[i] = bson.M{"_id.doctype": interval.start}
		}
	}
	return bson.M{"$or": filter}
}
