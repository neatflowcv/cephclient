package domain

type EntryGroup struct {
	olhs      []*OLHBIEntry
	plains    []*PlainBIEntry
	instances []*InstanceBIEntry
}

func NewEntryGroup(olhs []*OLHBIEntry, plains []*PlainBIEntry, instances []*InstanceBIEntry) *EntryGroup {
	copiedOLHs := make([]*OLHBIEntry, len(olhs))
	copy(copiedOLHs, olhs)

	copiedPlains := make([]*PlainBIEntry, len(plains))
	copy(copiedPlains, plains)

	copiedInstances := make([]*InstanceBIEntry, len(instances))
	copy(copiedInstances, instances)

	return &EntryGroup{
		olhs:      copiedOLHs,
		plains:    copiedPlains,
		instances: copiedInstances,
	}
}

func (g *EntryGroup) OLHs() []*OLHBIEntry {
	copied := make([]*OLHBIEntry, len(g.olhs))
	copy(copied, g.olhs)

	return copied
}

func (g *EntryGroup) Plains() []*PlainBIEntry {
	copied := make([]*PlainBIEntry, len(g.plains))
	copy(copied, g.plains)

	return copied
}

func (g *EntryGroup) Instances() []*InstanceBIEntry {
	copied := make([]*InstanceBIEntry, len(g.instances))
	copy(copied, g.instances)

	return copied
}
