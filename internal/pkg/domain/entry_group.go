package domain

type EntryGroup struct {
	olhs      []*OLH
	plains    []*Plain
	instances []*Instance
}

func NewEntryGroup(olhs []*OLH, plains []*Plain, instances []*Instance) *EntryGroup {
	copiedOLHs := make([]*OLH, len(olhs))
	copy(copiedOLHs, olhs)

	copiedPlains := make([]*Plain, len(plains))
	copy(copiedPlains, plains)

	copiedInstances := make([]*Instance, len(instances))
	copy(copiedInstances, instances)

	return &EntryGroup{
		olhs:      copiedOLHs,
		plains:    copiedPlains,
		instances: copiedInstances,
	}
}

func (g *EntryGroup) OLHs() []*OLH {
	copied := make([]*OLH, len(g.olhs))
	copy(copied, g.olhs)

	return copied
}

func (g *EntryGroup) Plains() []*Plain {
	copied := make([]*Plain, len(g.plains))
	copy(copied, g.plains)

	return copied
}

func (g *EntryGroup) Instances() []*Instance {
	copied := make([]*Instance, len(g.instances))
	copy(copied, g.instances)

	return copied
}

func (g *EntryGroup) IsEmpty() bool {
	return len(g.olhs) == 0 &&
		len(g.plains) == 0 &&
		len(g.instances) == 0
}

func (g *EntryGroup) Versions() []string {
	return g.versions()
}

func (g *EntryGroup) ExtractRawObjectNames(marker, objectName string) []string {
	var names []string

	name := NewOLHRawObjectName(marker, objectName).Value()
	names = append(names, name)

	for _, version := range g.versions() {
		if version == "" { // "" version의 RawObject는 OLH와 동일하다.
			continue
		}

		name := NewVersionRawObjectName(marker, version, objectName).Value()

		names = append(names, name)
	}

	return names
}

func (g *EntryGroup) ExtractOmapKeys() []string {
	var keys []string

	seen := map[string]struct{}{}

	for _, entry := range g.olhs {
		key := entry.IDX()
		if _, ok := seen[key]; ok {
			continue
		}

		seen[key] = struct{}{}
		keys = append(keys, key)
	}

	for _, entry := range g.plains {
		key := entry.IDX()
		if _, ok := seen[key]; ok {
			continue
		}

		seen[key] = struct{}{}
		keys = append(keys, key)
	}

	for _, entry := range g.instances {
		key := entry.IDX()
		if _, ok := seen[key]; ok {
			continue
		}

		seen[key] = struct{}{}
		keys = append(keys, key)
	}

	return keys
}

func (g *EntryGroup) versions() []string {
	var versions []string

	seen := NewSeen()

	for _, olh := range g.olhs {
		version := olh.Instance()
		if seen.Check(version) {
			continue
		}

		seen.Set(version)
		versions = append(versions, version)

		for _, pendingLog := range olh.PendingLog() {
			for _, item := range pendingLog.val {
				valVersion := item.Instance()
				if seen.Check(valVersion) {
					continue
				}

				seen.Set(valVersion)
				versions = append(versions, valVersion)
			}
		}
	}

	for _, entry := range g.instances {
		version := entry.Entry().Instance()
		if seen.Check(version) {
			continue
		}

		seen.Set(version)
		versions = append(versions, version)
	}

	for _, entry := range g.plains {
		version := entry.Entry().Instance()
		if seen.Check(version) {
			continue
		}

		seen.Set(version)
		versions = append(versions, version)
	}

	return versions
}
