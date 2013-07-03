package registry

import (
	"errors"
	"fmt"
	"log"
	"regexp"
	"strconv"
	"strings"
)

type hashWidth uint64
type windowSize int
type groupSize uint64
type addresses []string
type query string
type feeds []string

func checkErr(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func validateUint64(value string, minValue uint64, maxValue uint64, multiple uint64, name string) (uint64, error) {
	v, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		return 0, nil
	}
	if v < minValue || v > maxValue {
		return 0, errors.New(fmt.Sprintf("%s must be a value between %d and %d.", name, minValue, maxValue))
	}
	if v%multiple != 0 {
		return 0, errors.New(fmt.Sprintf("%s must be a multiple of %d.", name, multiple))
	}
	return v, nil
}

func (h *hashWidth) Set(value string) error {
	v, err := validateUint64(value, 12, 64, 1, "Hash Width")
	if err == nil {
		*h = hashWidth(v)
	}
	return err
}

func (h *hashWidth) String() string {
	return fmt.Sprintf("%d", *h)
}

func (w *windowSize) Set(value string) error {
	v, err := validateUint64(value, 8, 256, 1, "Window Size")
	if err == nil {
		*w = windowSize(v)
	}
	return err
}

func (w *windowSize) String() string {
	return fmt.Sprintf("%d", *w)
}

func (g *groupSize) Set(value string) error {
	v, err := validateUint64(value, 16, 256, 8, "Group Size")
	if err == nil {
		*g = groupSize(v)
	}
	return err
}

func (g *groupSize) String() string {
	return fmt.Sprintf("%d", *g)
}

func (f *feeds) Set(value string) error {
	*f = strings.Split(value, ",")
	return nil
}

func (f *feeds) String() string {
	return fmt.Sprintf("%v", *f)
}

func (a *addresses) Set(value string) error {
	sections := strings.Split(value, ",")
	l := uint32(len(sections))
	if l == 0 || (l&(l-1)) != 0 {
		return errors.New("Number of addresses must be a power of 2")
	}
	*a = sections
	return nil
}

func (a *addresses) String() string {
	return fmt.Sprintf("%v", *a)
}

func (q *query) Set(value string) error {
	matched, _ := regexp.MatchString("(((\\d+-\\d+):?|(\\d+):?))+}", value)
	if !matched {
		return errors.New("Query must be in form 1-2:3:4-5 ...")
	}
	*q = query(value)
	return nil
}

func (q *query) String() string {
	return string(*q)
}
