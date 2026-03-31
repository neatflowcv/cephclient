package domain

type Zone struct {
	dataPool  string
	indexPool string
}

func NewZone(dataPool, indexPool string) *Zone {
	return &Zone{
		dataPool:  dataPool,
		indexPool: indexPool,
	}
}

func (z *Zone) DataPool() string {
	return z.dataPool
}

func (z *Zone) IndexPool() string {
	return z.indexPool
}
