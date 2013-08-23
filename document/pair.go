package document

import (
	"bytes"
	"fmt"
	"sort"
)

type StepSlice []Step
type PositionSlice []int32

type Step struct {
	left   int32
	start  int
	length int
}

func (s StepSlice) Len() int           { return len(s) }
func (s StepSlice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s StepSlice) Less(i, j int) bool { return s[i].left < s[j].left }

func (s StepSlice) ShellSort() {
	size := len(s)
	for inc := size / 2; inc > 0; inc = (inc + 1) * 5 / 11 {
		for i := inc; i < size; i++ {
			j, temp := i, s[i]
			for ; j >= inc && s[j-inc].left > temp.left; j -= inc {
				s[j] = s[j-inc]
			}
			s[j] = temp
		}
	}
}

type Pairs struct {
	steps StepSlice
	right PositionSlice
}

func NewPairs(estimate int) *Pairs {
	return &Pairs{
		steps: make(StepSlice, 0, estimate),
		right: make(PositionSlice, 0, estimate),
	}
}

func (p *Pairs) Append(left int32, right PositionSlice) {
	p.steps = append(p.steps, Step{left: left, start: len(p.right), length: len(right)})
	p.right = append(p.right, right...)
}

func (p *Pairs) Sort() {
	p.steps.ShellSort()
	// sort.Sort(p.steps)
}

func (p *Pairs) String() string {
	var buf bytes.Buffer
	for _, s := range p.steps {
		buf.WriteString(fmt.Sprintf("%10d: %v\n", s.left, p.right[s.start:s.start+s.length]))
	}
	buf.WriteString(fmt.Sprintf("Steps Len: %d Capacity:%d Right Len:%d Capacity:%d", len(p.steps), cap(p.steps), len(p.right), cap(p.right)))
	return buf.String()
}

func (p *Pairs) BuildFragments(left *Document, windowSize, minLength int) (FragmentSlice, ThemeMap) {
	fragments, themes, text := make(FragmentSlice, 0, len(p.steps)), make(ThemeMap), left.NormalisedText()
	buildFragment := func(l, r, length int) {
		fragmentLength := length - r + windowSize
		if fragmentLength >= minLength {
			fragment, theme := newFragment(text, l, r, fragmentLength)
			if fragment.Length >= minLength {
				fragments = append(fragments, *fragment)
				themes[theme.Id] = *theme
			}
		}
	}
	for i, step := range p.steps {
		for _, r := range p.right[step.start : step.start+step.length] {
			length := r
		gobble:
			for _, next := range p.steps[i+1:] {
				for j, right := range p.right[next.start : next.start+next.length] {
					switch {
					case right == length+1:
						p.right[next.start+j] = -1
						length++
						continue gobble
					case right > length:
						break gobble
					}
				}
				break gobble
			}
			buildFragment(int(step.left), int(r), int(length))
		}
	}
	sort.Sort(fragments)
	return fragments, themes
}
