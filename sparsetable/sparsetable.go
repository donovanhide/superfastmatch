package sparsetable

import (
	"bytes"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"unsafe"
)

type SparseTable struct {
	groupSize uint64
	lengths   []uint8
	groups    [][]byte
}

func Init(size uint64, groupSize uint64) *SparseTable {
	groupCount := size / groupSize
	if (size % groupSize) != 0 {
		groupCount += 1
	}
	return &SparseTable{
		groupSize: groupSize,
		lengths:   make([]uint8, size),
		groups:    make([][]byte, groupCount),
	}
}

func (s SparseTable) Set(pos uint64, value []byte) error {
	if len(value) > 255 {
		return errors.New("Value is greater than 255 bytes.")
	}
	if pos > uint64(len(s.lengths)) {
		return errors.New("Position is out of bounds of this sparsetable")
	}
	group, start, end := s.getOffsets(pos)
	s.groups[group] = append(s.groups[group][:start], append(value, s.groups[group][end:]...)...)
	s.lengths[pos] = uint8(len(value))
	return nil
}

func (s SparseTable) Get(pos uint64) ([]byte, error) {
	if pos > uint64(len(s.lengths)) {
		return nil, errors.New("Position is out of bounds of this sparsetable")
	}
	group, start, end := s.getOffsets(pos)
	return s.groups[group][start:end], nil
}

func (s SparseTable) Remove(pos uint64) error {
	return s.Set(pos, []byte(""))
}

func (s SparseTable) getOffsets(pos uint64) (uint64, uint64, uint64) {
	group := pos / s.groupSize
	start := uint64(0)
	for _, value := range s.lengths[group*s.groupSize : pos] {
		start += uint64(value)
	}
	end := start + uint64(s.lengths[pos])
	// fmt.Printf("Group: %v Start: %v End: %v\n", group, start, end)
	return group, start, end
}

func (s SparseTable) Size() uint64 {
	return uint64(len(s.lengths))
}

func (s SparseTable) Count() uint64 {
	sum := uint64(0)
	for _, value := range s.lengths {
		if value > 0 {
			sum += 1
		}
	}
	return sum
}

func (s SparseTable) Memory() uint64 {
	return uint64(unsafe.Sizeof(s.lengths) + unsafe.Sizeof(s))
}

func (s SparseTable) String() string {
	var buffer bytes.Buffer
	groups := make([]string, len(s.groups))
	for i := range s.groups {
		groups[i] = strconv.Itoa(len(s.groups[i]))
	}
	buffer.WriteString(fmt.Sprintf("SparseTable Count:%v Size: %v\nGroups: [%s]\n", s.Count(), s.Size(), strings.Join(groups, ",")))
	for i := uint64(0); i < s.Size(); i++ {
		value, _ := s.Get(i)
		if len(value) > 0 {
			buffer.WriteString(fmt.Sprintf("%v: \"%s\"\n", i, value))
		}
	}
	return buffer.String()
}
