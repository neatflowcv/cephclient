package domain

type Instance struct {
	*dir
}

func NewInstance(p DirParams) *Instance {
	return &Instance{
		dir: newDir(p),
	}
}

func (i *Instance) Type() string {
	return "instance"
}
