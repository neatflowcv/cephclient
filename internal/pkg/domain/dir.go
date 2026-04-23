package domain

type dir struct {
	exists         bool
	flags          int
	instance       string
	locator        string
	meta           *BIObjectMeta
	name           string
	pending        bool
	tag            string
	ver            *BIVersion
	versionedEpoch int
	idx            *BIIndex
}

type DirParams struct {
	Exists         bool
	Flags          int
	Instance       string
	Locator        string
	Meta           *BIObjectMeta
	Name           string
	Pending        bool
	Tag            string
	Ver            *BIVersion
	VersionedEpoch int
	IDX            *BIIndex
}

func newDir(p DirParams) *dir {
	return &dir{
		exists:         p.Exists,
		flags:          p.Flags,
		instance:       p.Instance,
		locator:        p.Locator,
		meta:           p.Meta,
		name:           p.Name,
		pending:        p.Pending,
		tag:            p.Tag,
		ver:            p.Ver,
		versionedEpoch: p.VersionedEpoch,
		idx:            p.IDX,
	}
}

func (d *dir) IDX() string {
	return d.idx.Escaped()
}

func (d *dir) Exists() bool {
	return d.exists
}

func (d *dir) Flags() int {
	return d.flags
}

func (d *dir) Instance() string {
	return d.instance
}

func (d *dir) Locator() string {
	return d.locator
}

func (d *dir) Meta() *BIObjectMeta {
	return d.meta
}

func (d *dir) Name() string {
	return d.name
}

func (d *dir) Pending() bool {
	return d.pending
}

func (d *dir) Tag() string {
	return d.tag
}

func (d *dir) Ver() *BIVersion {
	return d.ver
}

func (d *dir) VersionedEpoch() int {
	return d.versionedEpoch
}
