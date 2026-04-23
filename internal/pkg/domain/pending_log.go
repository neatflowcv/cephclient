package domain

type PendingLog struct {
	key int
	val []PendingLogItem
}

type PendingLogParams struct {
	Key int
	Val []PendingLogItem
}

func NewPendingLog(p PendingLogParams) PendingLog {
	copiedVal := make([]PendingLogItem, len(p.Val))
	copy(copiedVal, p.Val)

	return PendingLog{
		key: p.Key,
		val: copiedVal,
	}
}

func (l PendingLog) Key() int {
	return l.key
}

func (l PendingLog) Val() []PendingLogItem {
	copied := make([]PendingLogItem, len(l.val))
	copy(copied, l.val)

	return copied
}

type PendingLogItem struct {
	deleteMarker bool
	epoch        int
	instance     string
	name         string
	op           string
	opTag        string
}

func NewPendingLogItem(
	epoch int,
	op, opTag, name, instance string,
	deleteMarker bool,
) PendingLogItem {
	return PendingLogItem{
		deleteMarker: deleteMarker,
		epoch:        epoch,
		instance:     instance,
		name:         name,
		op:           op,
		opTag:        opTag,
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
