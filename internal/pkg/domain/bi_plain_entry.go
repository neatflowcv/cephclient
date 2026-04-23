package domain

type PlainBIEntry struct {
	*dir
}

func NewPlainBIEntry(p DirParams) *PlainBIEntry {
	return &PlainBIEntry{
		dir: newDir(p),
	}
}

func (e *PlainBIEntry) Type() string {
	return "plain"
}
