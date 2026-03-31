package domain

type Layout struct {
	generation int
}

func NewLayout(generation int) *Layout {
	return &Layout{generation: generation}
}

func (l *Layout) Generation() int {
	return l.generation
}
