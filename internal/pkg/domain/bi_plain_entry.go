package domain

type PlainBIEntry struct {
	entry *BIObjectEntry
	idx   *BIIndex
}

func NewPlainBIEntry(idx *BIIndex, entry *BIObjectEntry) *PlainBIEntry {
	return &PlainBIEntry{
		entry: entry,
		idx:   idx,
	}
}

func (e *PlainBIEntry) Entry() *BIObjectEntry {
	return e.entry
}

func (e *PlainBIEntry) IDX() *BIIndex {
	return e.idx
}

func (e *PlainBIEntry) Type() string {
	return "plain"
}
