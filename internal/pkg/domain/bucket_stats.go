package domain

type BucketStats struct {
	id string
}

func NewBucketStats(id string) *BucketStats {
	return &BucketStats{id: id}
}

func (b *BucketStats) ID() string {
	return b.id
}
