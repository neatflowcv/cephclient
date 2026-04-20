package domain

type Seen struct {
	values map[string]struct{}
}

func NewSeen() *Seen {
	return &Seen{
		values: map[string]struct{}{},
	}
}

func (s *Seen) Check(key string) bool {
	_, ok := s.values[key]

	return ok
}

func (s *Seen) Set(key string) {
	s.values[key] = struct{}{}
}
