package domain

type BIPendingLogEntry struct {
	key int
	val []BIPendingLogItem
}

func NewBIPendingLogEntry(key int, val []BIPendingLogItem) BIPendingLogEntry {
	copiedVal := make([]BIPendingLogItem, len(val))
	copy(copiedVal, val)

	return BIPendingLogEntry{
		key: key,
		val: copiedVal,
	}
}

func (e BIPendingLogEntry) Key() int {
	return e.key
}

func (e BIPendingLogEntry) Val() []BIPendingLogItem {
	copied := make([]BIPendingLogItem, len(e.val))
	copy(copied, e.val)

	return copied
}

type BIPendingLogItem struct {
	deleteMarker bool
	epoch        int
	instance     string
	name         string
	op           string
	opTag        string
}

func NewBIPendingLogItem(
	epoch int,
	op, opTag, name, instance string,
	deleteMarker bool,
) BIPendingLogItem {
	return BIPendingLogItem{
		deleteMarker: deleteMarker,
		epoch:        epoch,
		instance:     instance,
		name:         name,
		op:           op,
		opTag:        opTag,
	}
}

func (i BIPendingLogItem) DeleteMarker() bool {
	return i.deleteMarker
}

func (i BIPendingLogItem) Epoch() int {
	return i.epoch
}

func (i BIPendingLogItem) Instance() string {
	return i.instance
}

func (i BIPendingLogItem) Name() string {
	return i.name
}

func (i BIPendingLogItem) Op() string {
	return i.op
}

func (i BIPendingLogItem) OpTag() string {
	return i.opTag
}
