package sparsetable

import (
	"bytes"
	"fmt"
	"io"
	"strconv"
	"strings"
	"unsafe"
)

func SumUint8(x []uint8) uint64 {
	sum := uint64(0)
	for _, v := range x {
		sum += uint64(v)
	}
	return sum
}

func FastSumUint8(x []uint8) uint64

const MAX_SIZE = 255

type Error struct {
	pos         uint64
	Full        bool
	OutOfBounds bool
	ShortRead   bool
}

func (e *Error) Error() string {
	msg := ""
	switch {
	case e.Full:
		msg = "Sparsetable full for position %v"
	case e.OutOfBounds:
		msg = "Position %v out of bounds of sparsetable"
	case e.ShortRead:
		msg = "Short Read for position %v"
	}
	return fmt.Sprintf(msg, e.pos)
}

type SparseTable struct {
	groupSize uint64
	lengths   []uint8
	groups    [][]byte
	buffer    []byte
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
		buffer:    make([]byte, MAX_SIZE),
	}
}

func (s *SparseTable) getOffsets(pos uint64) (uint64, uint64, uint64) {
	group := pos / s.groupSize
	start := FastSumUint8(s.lengths[group*s.groupSize : pos])
	//start := SumUint8(s.lengths[group*s.groupSize : pos])
	end := start + uint64(s.lengths[pos])
	return group, start, end
}

func (s *SparseTable) SetBytes(pos uint64, b []byte) error {
	r := bytes.NewReader(b)
	return s.Set(pos, r, len(b))
}

func (s *SparseTable) Set(pos uint64, r io.Reader, length int) error {
	if pos > uint64(len(s.lengths)) {
		return &Error{pos: pos, OutOfBounds: true}
	}
	if length >= MAX_SIZE {
		return &Error{pos: pos, Full: true}
	}

	group, start, end := s.getOffsets(pos)

	current := s.lengths[pos]
	g := &s.groups[group]
	diff := length - int(current)
	switch {
	case diff > 0 && cap(*g) > (len(*g)+diff):
		*g = (*g)[:len(*g)+diff]
		copy((*g)[int(end)+diff:], (*g)[end:])
		break
	case diff > 0:
		*g = append(*g, s.buffer[:length]...)
		copy((*g)[int(end)+diff:], (*g)[end:])
		break
	case diff < 0:
		copy((*g)[int(end)+diff:], (*g)[end:])
		*g = (*g)[:len(*g)+diff]
		break
	}
	s.lengths[pos] = uint8(length)

	n, err := r.Read((*g)[start : int(start)+length])
	if n != length {
		return &Error{pos: pos, ShortRead: true}
	}
	if err == io.EOF {
		return nil
	}
	return err
}

func (s *SparseTable) Get(pos uint64, w io.Writer) error {
	if pos > uint64(len(s.lengths)) {
		return &Error{pos: pos, OutOfBounds: true}
	}
	group, start, end := s.getOffsets(pos)
	_, err := w.Write(s.groups[group][start:end])
	return err
}

func (s *SparseTable) GetBytes(pos uint64) ([]byte, error) {
	buf := new(bytes.Buffer)
	err := s.Get(pos, buf)
	return buf.Bytes(), err
}

func (s *SparseTable) Remove(pos uint64) error {
	b := []byte(nil)
	return s.Set(pos, bytes.NewReader(b), len(b))
}

func (s *SparseTable) Size() uint64 {
	return uint64(len(s.lengths))
}

func (s *SparseTable) Count() uint64 {
	count := uint64(0)
	for i := range s.lengths {
		if s.lengths[i] > 0 {
			count++
		}
	}
	return count
}

func (s *SparseTable) Memory() uint64 {
	return uint64(unsafe.Sizeof(s.lengths) + unsafe.Sizeof(s))
}

func (s *SparseTable) String() string {
	var buffer bytes.Buffer
	groups := make([]string, len(s.groups))
	for i := range s.groups {
		groups[i] = strconv.Itoa(len(s.groups[i]))
	}
	buffer.WriteString(fmt.Sprintf("SparseTable Count:%v Size: %v\nGroups: [%s]\n", s.Count(), s.Size(), strings.Join(groups, ",")))
	buf := new(bytes.Buffer)
	for i := uint64(0); i < s.Size(); i++ {
		s.Get(i, buf)
		if buf.Len() > 0 {
			buffer.WriteString(fmt.Sprintf("%v: \"%s\"\n", i, buf.String()))
			buf.Reset()
		}
	}
	return buffer.String()
}

func (s *SparseTable) Stats() interface{} {
	return map[string]uint64{
		"size":      s.Size(),
		"count":     s.Count(),
		"groupSize": s.groupSize,
	}
}
