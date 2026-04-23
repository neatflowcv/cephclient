package domain

type InstanceBIEntry struct {
	*dir
}

func NewInstanceBIEntry(p DirParams) *InstanceBIEntry {
	return &InstanceBIEntry{
		dir: newDir(p),
	}
}

func (e *InstanceBIEntry) Type() string {
	return "instance"
}
