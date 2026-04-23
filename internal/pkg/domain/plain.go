package domain

type Plain struct {
	*dir
}

func NewPlain(p DirParams) *Plain {
	return &Plain{
		dir: newDir(p),
	}
}

func (p *Plain) Type() string {
	return "plain"
}
