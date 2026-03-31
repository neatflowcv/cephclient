package domain

type InstanceBIEntry struct {
	entry *BIObjectEntry
	idx   *BIIndex
}

func NewInstanceBIEntry(idx *BIIndex, entry *BIObjectEntry) *InstanceBIEntry {
	return &InstanceBIEntry{
		entry: entry,
		idx:   idx,
	}
}

func (e *InstanceBIEntry) Entry() *BIObjectEntry {
	return e.entry
}

func (e *InstanceBIEntry) IDX() *BIIndex {
	return e.idx
}

func (e *InstanceBIEntry) Type() string {
	return "instance"
}
