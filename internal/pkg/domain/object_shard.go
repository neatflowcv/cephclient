package domain

type ObjectShard struct {
	shard int
}

func NewObjectShard(shard int) *ObjectShard {
	return &ObjectShard{shard: shard}
}

func (o *ObjectShard) Shard() int {
	return o.shard
}
