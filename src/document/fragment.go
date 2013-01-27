package document

import (
	"bytes"
	"exp/utf8string"
	"fmt"
	"hash/fnv"
	"reflect"
	"registry"
	"sort"
	"strings"
	"unicode"
	"unicode/utf8"
)

type ThemeId uint64

type Theme struct {
	Id   ThemeId `json:"id" bson:"_id"`
	Text string  `json:"text"`
}

type ThemeMap map[ThemeId]Theme
type ThemeSlice []Theme

func (t ThemeSlice) Len() int           { return len(t) }
func (t ThemeSlice) Swap(i, j int)      { t[i], t[j] = t[j], t[i] }
func (t ThemeSlice) Less(i, j int) bool { return t[i].Id > t[j].Id }

type Fragment struct {
	Left   int
	Right  int
	Length int
	Id     ThemeId
}

type FragmentSlice []Fragment

func (f FragmentSlice) Len() int      { return len(f) }
func (f FragmentSlice) Swap(i, j int) { f[i], f[j] = f[j], f[i] }
func (f FragmentSlice) Less(i, j int) bool {
	l, r := f[i], f[j]
	switch {
	case l.Length != r.Length:
		return l.Length > r.Length
	case l.Left != r.Left:
		return l.Left < r.Left
	}
	return l.Right < r.Right
}

func newTheme(text string) *Theme {
	hasher := fnv.New32a()
	hasher.Write([]byte(text))
	return &Theme{
		Id:   ThemeId(uint64(hasher.Sum32()) | (uint64(len(text)) << 32)),
		Text: text,
	}
}

func (m ThemeMap) Save(registry *registry.Registry) error {
	return registry.C("theme").Insert(reflect.ValueOf(m).MapKeys())
}

func (m ThemeMap) Sort() ThemeSlice {
	i, themes := 0, make(ThemeSlice, len(m))
	for _, v := range m {
		themes[i] = v
		i++
	}
	sort.Sort(themes)
	return themes
}

func (m ThemeMap) String() string {
	var buf bytes.Buffer
	for _, t := range m.Sort() {
		buf.WriteString(fmt.Sprintf("%d,\"%.80s\",\n", t.Id, t.Text))
	}
	return buf.String()
}

func (s FragmentSlice) Flip() {
	for i := range s {
		s[i].Left, s[i].Right = s[i].Right, s[i].Left
	}
}

func (s FragmentSlice) String(m ThemeMap) string {
	var buf bytes.Buffer
	for _, f := range s {
		t := m[f.Id]
		buf.WriteString(fmt.Sprintf("%d,%d,%d,%d,\"%.80s\"\n", f.Left, f.Right, f.Length, t.Id, t.Text))
	}
	return buf.String()
}

func notWhitespace(r rune) bool {
	return unicode.IsLetter(r) || unicode.IsDigit(r)
}

func newFragment(text *utf8string.String, left, right, length int) (*Fragment, *Theme) {
	match := text.Slice(left, left+length)
	if trimLeft := strings.IndexFunc(match, notWhitespace); trimLeft != -1 {
		unicodeTrim := utf8.RuneCountInString(match[:trimLeft])
		match = match[trimLeft:]
		left += unicodeTrim
		right += unicodeTrim
		length -= unicodeTrim
	}
	if trimmedLength := strings.LastIndexFunc(match, notWhitespace) + 1; trimmedLength != 0 {
		unicodeTrim := utf8.RuneCountInString(match[trimmedLength:])
		match = match[:trimmedLength]
		length -= unicodeTrim
	}
	theme := newTheme(match)
	return &Fragment{
		Left:   left,
		Right:  right,
		Length: length,
		Id:     theme.Id,
	}, theme
}

func (f *Fragment) Pretty(textLimit int, left *utf8string.String) string {
	length := min(textLimit-3, f.Length)
	cleaner := strings.NewReplacer("\n", " ")
	text := cleaner.Replace(left.Slice(f.Left, f.Left+length))
	if length < f.Length {
		text = text + "..."
	}
	return fmt.Sprintf("Left: %8d\tRight: %8d\tLength: %8d\tText: %s\n", f.Left, f.Right, f.Length, text)
}

func (f *Fragment) String(left, right *utf8string.String) string {
	l, r := f.Strings(left, right)
	return fmt.Sprintf("\"%s\" \"%s\"", l, r)
}

func (f *Fragment) Strings(left, right *utf8string.String) (string, string) {
	return left.Slice(f.Left, f.Left+f.Length), right.Slice(f.Right, f.Right+f.Length)
}

func (f *Fragment) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf("[%d,%d,%d,%d]", f.Left, f.Right, f.Length, f.Id)), nil
}

func (f *Fragment) UnmarshalJSON(b []byte) error {
	_, err := fmt.Sscanf(string(b), "[%d,%d,%d,%d]", &f.Left, &f.Right, &f.Length, &f.Id)
	return err
}
