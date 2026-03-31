package domain

type BucketStats struct {
	id          string
	totalShards int
}

func NewBucketStats(id string, totalShards int) *BucketStats {
	return &BucketStats{
		id:          id,
		totalShards: totalShards,
	}
}

func (b *BucketStats) ID() string {
	return b.id
}

func (b *BucketStats) TotalShards() int {
	return b.totalShards
}
