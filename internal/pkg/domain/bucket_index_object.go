package domain

import "fmt"

type BucketIndexObject struct {
	marker string
	layout int
	shard  int
}

func NewBucketIndexObject(marker string, layout, shard int) *BucketIndexObject {
	return &BucketIndexObject{
		marker: marker,
		layout: layout,
		shard:  shard,
	}
}

func (o *BucketIndexObject) Marker() string {
	return o.marker
}

func (o *BucketIndexObject) Shard() int {
	return o.shard
}

func (o *BucketIndexObject) Layout() int {
	return o.layout
}

func (o *BucketIndexObject) Raw() string {
	if o.layout > 0 {
		return fmt.Sprintf(".dir.%s.%d.%d", o.marker, o.layout, o.shard)
	}

	return fmt.Sprintf(".dir.%s.%d", o.marker, o.shard)
}
