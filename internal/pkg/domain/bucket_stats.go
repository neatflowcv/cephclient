package domain

type BucketStats struct {
	id          string
	totalShards int
	marker      string
	versioning  VersioningStatus
}

func NewBucketStats(
	bucketID string,
	totalShards int,
	marker string,
	versioningStatus VersioningStatus,
) (*BucketStats, error) {
	err := versioningStatus.Validate()
	if err != nil {
		return nil, err
	}

	return &BucketStats{
		id:          bucketID,
		totalShards: totalShards,
		marker:      marker,
		versioning:  versioningStatus,
	}, nil
}

func (b *BucketStats) ID() string {
	return b.id
}

func (b *BucketStats) TotalShards() int {
	return b.totalShards
}

func (b *BucketStats) Marker() string {
	return b.marker
}

func (b *BucketStats) Versioning() VersioningStatus {
	return b.versioning
}
