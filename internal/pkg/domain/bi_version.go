package domain

type BIVersion struct {
	epoch int
	pool  int
}

func NewBIVersion(pool, epoch int) *BIVersion {
	return &BIVersion{
		epoch: epoch,
		pool:  pool,
	}
}

func (v *BIVersion) Epoch() int {
	return v.epoch
}

func (v *BIVersion) Pool() int {
	return v.pool
}
