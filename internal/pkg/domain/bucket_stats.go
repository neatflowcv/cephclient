package domain

type BucketStats struct {
	id          string
	name        string
	totalShards int
	marker      string
	size        int64
	objectCount int
	versioning  VersioningStatus
}

func NewBucketStats(
	bucketID string,
	name string,
	totalShards int,
	marker string,
	size int64,
	objectCount int,
	versioningStatus VersioningStatus,
) (*BucketStats, error) {
	err := versioningStatus.Validate()
	if err != nil {
		return nil, err
	}

	return &BucketStats{
		id:          bucketID,
		name:        name,
		totalShards: totalShards,
		marker:      marker,
		size:        size,
		objectCount: objectCount,
		versioning:  versioningStatus,
	}, nil
}

func (b *BucketStats) ID() string {
	return b.id
}

func (b *BucketStats) Name() string {
	return b.name
}

func (b *BucketStats) TotalShards() int {
	return b.totalShards
}

func (b *BucketStats) Marker() string {
	return b.marker
}

func (b *BucketStats) Size() int64 {
	return b.size
}

func (b *BucketStats) ObjectCount() int {
	return b.objectCount
}

func (b *BucketStats) Versioning() VersioningStatus {
	return b.versioning
}
