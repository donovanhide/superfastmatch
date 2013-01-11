package document

import (
	"sort"
)

type StepSlice []Step
type PositionSlice sort.IntSlice

type Step struct {
	left   int
	length int
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

func (p *Pairs) Append(left int, right PositionSlice) {
	p.steps = append(p.steps, Step{left: left, length: len(right)})
	p.right = append(p.right, right...)
}

func (p *Pairs) BuildFragments(left *Document, windowSize, minLength int) (FragmentSlice, ThemeMap) {
	fragments, themes := make(FragmentSlice, 0, len(p.steps)), make(ThemeMap)
	buildFragment := func(l, r, length int) {
		fragmentLength := length - r + windowSize - 1
		if fragmentLength >= minLength {
			fragment, theme := newFragment(left.NormalisedText(), l, r, fragmentLength)
			if fragment.Length >= minLength {
				fragments = append(fragments, *fragment)
				themes[theme.Id] = *theme
			}
		}
	}
	counter := 0
	// Please don't ever ask me to explain this
	// It works and doesn't use maps
	// Therefore it is fast and memory efficient :-)
	for i, step := range p.steps {
		for _, r := range p.right[counter : counter+step.length] {
			length, offset := r+1, counter+step.length
		gobble:
			for _, next := range p.steps[i+1:] {
				for j, right := range p.right[offset : offset+next.length] {
					switch {
					case right == length:
						p.right[offset+j] = -1
						offset += next.length
						length++
						continue gobble
					case right > length:
						break gobble
					}
				}
				break gobble
			}
			buildFragment(step.left, r, length)
		}
		counter += step.length
	}
	sort.Sort(fragments)
	return fragments, themes
}
