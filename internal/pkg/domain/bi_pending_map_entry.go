package domain

type BIPendingMapEntry struct {
	key string
	val *BIPendingMapValue
}

func NewBIPendingMapEntry(key string, val *BIPendingMapValue) BIPendingMapEntry {
	return BIPendingMapEntry{
		key: key,
		val: val,
	}
}

func (e BIPendingMapEntry) Key() string {
	return e.key
}

func (e BIPendingMapEntry) Val() *BIPendingMapValue {
	return e.val
}

type BIPendingMapValue struct {
	op        int
	state     int
	timestamp string
}

func NewBIPendingMapValue(state int, timestamp string, op int) *BIPendingMapValue {
	return &BIPendingMapValue{
		op:        op,
		state:     state,
		timestamp: timestamp,
	}
}

func (v *BIPendingMapValue) Op() int {
	return v.op
}

func (v *BIPendingMapValue) State() int {
	return v.state
}

func (v *BIPendingMapValue) Timestamp() string {
	return v.timestamp
}
