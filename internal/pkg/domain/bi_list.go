package domain

type BIList struct {
	entries []BIEntry
}

func NewBIList(entries []BIEntry) *BIList {
	copied := make([]BIEntry, len(entries))
	copy(copied, entries)

	return &BIList{entries: copied}
}

func (l *BIList) Entries() []BIEntry {
	copied := make([]BIEntry, len(l.entries))
	copy(copied, l.entries)

	return copied
}
