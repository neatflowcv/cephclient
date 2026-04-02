package domain

type BucketStats struct {
	id          string
	totalShards int
	versioning  Versioning
}

func NewBucketStats(bucketID string, totalShards int, versioning string) (*BucketStats, error) {
	bucketVersioning, err := NewVersioning(versioning)
	if err != nil {
		return nil, err
	}

	return &BucketStats{
		id:          bucketID,
		totalShards: totalShards,
		versioning:  bucketVersioning,
	}, nil
}

func (b *BucketStats) ID() string {
	return b.id
}

func (b *BucketStats) TotalShards() int {
	return b.totalShards
}

func (b *BucketStats) Versioning() Versioning {
	return b.versioning
}
