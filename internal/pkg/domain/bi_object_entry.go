package domain

type BIObjectEntry struct {
	exists         bool
	flags          int
	instance       string
	locator        string
	meta           *BIObjectMeta
	name           string
	pendingMap     []BIPendingMapEntry
	tag            string
	ver            *BIVersion
	versionedEpoch int
}

func NewBIObjectEntry(
	name, instance string,
	ver *BIVersion,
	locator string,
	exists bool,
	meta *BIObjectMeta,
	tag string,
	flags int,
	pendingMap []BIPendingMapEntry,
	versionedEpoch int,
) *BIObjectEntry {
	copiedPendingMap := make([]BIPendingMapEntry, len(pendingMap))
	copy(copiedPendingMap, pendingMap)

	return &BIObjectEntry{
		exists:         exists,
		flags:          flags,
		instance:       instance,
		locator:        locator,
		meta:           meta,
		name:           name,
		pendingMap:     copiedPendingMap,
		tag:            tag,
		ver:            ver,
		versionedEpoch: versionedEpoch,
	}
}

func (e *BIObjectEntry) Exists() bool {
	return e.exists
}

func (e *BIObjectEntry) Flags() int {
	return e.flags
}

func (e *BIObjectEntry) Instance() string {
	return e.instance
}

func (e *BIObjectEntry) Locator() string {
	return e.locator
}

func (e *BIObjectEntry) Meta() *BIObjectMeta {
	return e.meta
}

func (e *BIObjectEntry) Name() string {
	return e.name
}

func (e *BIObjectEntry) PendingMap() []BIPendingMapEntry {
	copied := make([]BIPendingMapEntry, len(e.pendingMap))
	copy(copied, e.pendingMap)

	return copied
}

func (e *BIObjectEntry) Tag() string {
	return e.tag
}

func (e *BIObjectEntry) Ver() *BIVersion {
	return e.ver
}

func (e *BIObjectEntry) VersionedEpoch() int {
	return e.versionedEpoch
}
