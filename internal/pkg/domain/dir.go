package domain

type dir struct {
	entry *BIObjectEntry
	idx   *BIIndex
}

type DirParams struct {
	Entry *BIObjectEntry
	IDX   *BIIndex
}

func newDir(p DirParams) *dir {
	return &dir{
		entry: p.Entry,
		idx:   p.IDX,
	}
}

func (d *dir) Entry() *BIObjectEntry {
	return d.entry
}

func (d *dir) IDX() string {
	return d.idx.Escaped()
}
