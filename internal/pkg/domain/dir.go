package domain

type dir struct {
	accountedSize    int
	appendable       bool
	category         int
	contentType      string
	etag             string
	exists           bool
	flags            int
	instance         string
	locator          string
	mtime            string
	name             string
	owner            string
	ownerDisplayName string
	pending          bool
	size             int
	storageClass     string
	tag              string
	userData         string
	ver              *BIVersion
	versionedEpoch   int
	idx              *BIIndex
}

type DirParams struct {
	AccountedSize    int
	Appendable       bool
	Category         int
	ContentType      string
	ETag             string
	Exists           bool
	Flags            int
	Instance         string
	Locator          string
	MTime            string
	Name             string
	Owner            string
	OwnerDisplayName string
	Pending          bool
	Size             int
	StorageClass     string
	Tag              string
	UserData         string
	Ver              *BIVersion
	VersionedEpoch   int
	IDX              *BIIndex
}

func newDir(p DirParams) *dir {
	return &dir{
		accountedSize:    p.AccountedSize,
		appendable:       p.Appendable,
		category:         p.Category,
		contentType:      p.ContentType,
		etag:             p.ETag,
		exists:           p.Exists,
		flags:            p.Flags,
		instance:         p.Instance,
		locator:          p.Locator,
		mtime:            p.MTime,
		name:             p.Name,
		owner:            p.Owner,
		ownerDisplayName: p.OwnerDisplayName,
		pending:          p.Pending,
		size:             p.Size,
		storageClass:     p.StorageClass,
		tag:              p.Tag,
		userData:         p.UserData,
		ver:              p.Ver,
		versionedEpoch:   p.VersionedEpoch,
		idx:              p.IDX,
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

func (d *dir) MTime() string {
	return d.mtime
}
