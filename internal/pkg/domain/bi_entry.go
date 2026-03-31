package domain

type BIEntry interface {
	IDX() *BIIndex
	Type() string
}
