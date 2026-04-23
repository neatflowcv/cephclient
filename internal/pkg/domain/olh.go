package domain

type OLH struct {
	deleteMarker   bool
	epoch          int
	exists         bool
	instance       string
	name           string
	pendingLog     []BIPendingLogEntry
	pendingRemoval bool
	tag            string
	idx            *BIIndex
}

type OLHParams struct {
	DeleteMarker   bool
	Epoch          int
	Exists         bool
	Instance       string
	Name           string
	PendingLog     []BIPendingLogEntry
	PendingRemoval bool
	Tag            string
	IDX            *BIIndex
}

func NewOLH(p OLHParams) *OLH {
	copiedPendingLog := make([]BIPendingLogEntry, len(p.PendingLog))
	copy(copiedPendingLog, p.PendingLog)

	return &OLH{
		deleteMarker:   p.DeleteMarker,
		epoch:          p.Epoch,
		exists:         p.Exists,
		instance:       p.Instance,
		name:           p.Name,
		pendingLog:     copiedPendingLog,
		pendingRemoval: p.PendingRemoval,
		tag:            p.Tag,
		idx:            p.IDX,
	}
}

func (e *OLH) IDX() *BIIndex {
	return e.idx
}

func (e *OLH) DeleteMarker() bool {
	return e.deleteMarker
}

func (e *OLH) Epoch() int {
	return e.epoch
}

func (e *OLH) Exists() bool {
	return e.exists
}

func (e *OLH) Name() string {
	return e.name
}

func (e *OLH) Instance() string {
	return e.instance
}

func (e *OLH) PendingLog() []BIPendingLogEntry {
	copied := make([]BIPendingLogEntry, len(e.pendingLog))
	copy(copied, e.pendingLog)

	return copied
}

func (e *OLH) ReferencedVersions() []string {
	versions := []string{e.Instance()}

	for _, pending := range e.PendingLog() {
		for _, item := range pending.Val() {
			versions = append(versions, item.Instance())
		}
	}

	return versions
}

func (e *OLH) PendingRemoval() bool {
	return e.pendingRemoval
}

func (e *OLH) Tag() string {
	return e.tag
}

func (e *OLH) Type() string {
	return "olh"
}
