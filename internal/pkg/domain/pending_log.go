package domain

type PendingLog struct {
	key int
	val []PendingLogItem
}

type PendingLogParams struct {
	Key int
	Val []PendingLogItemParams
}

func newPendingLog(p PendingLogParams) PendingLog {
	var copiedVal []PendingLogItem
	for _, item := range p.Val {
		copiedVal = append(copiedVal, newPendingLogItem(item))
	}

	return PendingLog{
		key: p.Key,
		val: copiedVal,
	}
}

func (l PendingLog) Key() int {
	return l.key
}

type PendingLogItem struct {
	deleteMarker bool
	epoch        int
	instance     string
	name         string
	op           string
	opTag        string
}

type PendingLogItemParams struct {
	DeleteMarker bool
	Epoch        int
	Instance     string
	Name         string
	Op           string
	OpTag        string
}

func newPendingLogItem(p PendingLogItemParams) PendingLogItem {
	return PendingLogItem{
		deleteMarker: p.DeleteMarker,
		epoch:        p.Epoch,
		instance:     p.Instance,
		name:         p.Name,
		op:           p.Op,
		opTag:        p.OpTag,
	}
}

func (i PendingLogItem) DeleteMarker() bool {
	return i.deleteMarker
}

func (i PendingLogItem) Epoch() int {
	return i.epoch
}

func (i PendingLogItem) Instance() string {
	return i.instance
}

func (i PendingLogItem) Name() string {
	return i.name
}

func (i PendingLogItem) Op() string {
	return i.op
}

func (i PendingLogItem) OpTag() string {
	return i.opTag
}
