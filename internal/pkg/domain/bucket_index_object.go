package domain

import "fmt"

type BucketIndexObject struct {
	marker string
	shard  int
}

func NewBucketIndexObject(marker string, shard int) *BucketIndexObject {
	return &BucketIndexObject{
		marker: marker,
		shard:  shard,
	}
}

func (o *BucketIndexObject) Marker() string {
	return o.marker
}

func (o *BucketIndexObject) Shard() int {
	return o.shard
}

func (o *BucketIndexObject) Raw() string {
	return fmt.Sprintf(".dir.%s.%d", o.marker, o.shard)
}
