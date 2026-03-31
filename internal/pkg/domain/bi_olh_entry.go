package domain

type OLHBIEntry struct {
	entry *BIOLHEntry
	idx   *BIIndex
}

func NewOLHBIEntry(idx *BIIndex, entry *BIOLHEntry) *OLHBIEntry {
	return &OLHBIEntry{
		entry: entry,
		idx:   idx,
	}
}

func (e *OLHBIEntry) Entry() *BIOLHEntry {
	return e.entry
}

func (e *OLHBIEntry) IDX() *BIIndex {
	return e.idx
}

func (e *OLHBIEntry) Type() string {
	return "olh"
}

type BIOLHEntry struct {
	deleteMarker   bool
	epoch          int
	exists         bool
	key            *BIOLHKey
	pendingLog     []BIPendingLogEntry
	pendingRemoval bool
	tag            string
}

func NewBIOLHEntry(
	key *BIOLHKey,
	deleteMarker bool,
	epoch int,
	pendingLog []BIPendingLogEntry,
	tag string,
	exists, pendingRemoval bool,
) *BIOLHEntry {
	copiedPendingLog := make([]BIPendingLogEntry, len(pendingLog))
	copy(copiedPendingLog, pendingLog)

	return &BIOLHEntry{
		deleteMarker:   deleteMarker,
		epoch:          epoch,
		exists:         exists,
		key:            key,
		pendingLog:     copiedPendingLog,
		pendingRemoval: pendingRemoval,
		tag:            tag,
	}
}

func (e *BIOLHEntry) DeleteMarker() bool {
	return e.deleteMarker
}

func (e *BIOLHEntry) Epoch() int {
	return e.epoch
}

func (e *BIOLHEntry) Exists() bool {
	return e.exists
}

func (e *BIOLHEntry) Key() *BIOLHKey {
	return e.key
}

func (e *BIOLHEntry) PendingLog() []BIPendingLogEntry {
	copied := make([]BIPendingLogEntry, len(e.pendingLog))
	copy(copied, e.pendingLog)

	return copied
}

func (e *BIOLHEntry) PendingRemoval() bool {
	return e.pendingRemoval
}

func (e *BIOLHEntry) Tag() string {
	return e.tag
}

type BIOLHKey struct {
	instance string
	name     string
}

func NewBIOLHKey(name, instance string) *BIOLHKey {
	return &BIOLHKey{
		instance: instance,
		name:     name,
	}
}

func (k *BIOLHKey) Instance() string {
	return k.instance
}

func (k *BIOLHKey) Name() string {
	return k.name
}
