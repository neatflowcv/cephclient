package domain

type OLH struct {
	deleteMarker   bool
	epoch          int
	exists         bool
	instance       string
	name           string
	pendingLog     []PendingLog
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
	PendingLog     []PendingLogParams
	PendingRemoval bool
	Tag            string
	IDX            *BIIndex
}

func NewOLH(p OLHParams) *OLH {
	var copiedPendingLog []PendingLog
	for _, pendingLog := range p.PendingLog {
		copiedPendingLog = append(copiedPendingLog, NewPendingLog(pendingLog))
	}

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

func (o *OLH) IDX() *BIIndex {
	return o.idx
}

func (o *OLH) DeleteMarker() bool {
	return o.deleteMarker
}

func (o *OLH) Epoch() int {
	return o.epoch
}

func (o *OLH) Exists() bool {
	return o.exists
}

func (o *OLH) Name() string {
	return o.name
}

func (o *OLH) Instance() string {
	return o.instance
}

func (o *OLH) PendingLog() []PendingLog {
	copied := make([]PendingLog, len(o.pendingLog))
	copy(copied, o.pendingLog)

	return copied
}

func (o *OLH) ReferencedVersions() []string {
	versions := []string{o.Instance()}

	for _, pending := range o.PendingLog() {
		for _, item := range pending.val {
			versions = append(versions, item.Instance())
		}
	}

	return versions
}

func (o *OLH) PendingRemoval() bool {
	return o.pendingRemoval
}

func (o *OLH) Tag() string {
	return o.tag
}

func (o *OLH) Type() string {
	return "olh"
}
