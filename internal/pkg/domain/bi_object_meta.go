package domain

type BIObjectMeta struct {
	accountedSize    int
	appendable       bool
	category         int
	contentType      string
	etag             string
	mtime            string
	owner            string
	ownerDisplayName string
	size             int
	storageClass     string
	userData         string
}

func NewBIObjectMeta(
	category, size int,
	mtime, etag, storageClass, owner, ownerDisplayName, contentType string,
	accountedSize int,
	userData string,
	appendable bool,
) *BIObjectMeta {
	return &BIObjectMeta{
		accountedSize:    accountedSize,
		appendable:       appendable,
		category:         category,
		contentType:      contentType,
		etag:             etag,
		mtime:            mtime,
		owner:            owner,
		ownerDisplayName: ownerDisplayName,
		size:             size,
		storageClass:     storageClass,
		userData:         userData,
	}
}

func (m *BIObjectMeta) AccountedSize() int {
	return m.accountedSize
}

func (m *BIObjectMeta) Appendable() bool {
	return m.appendable
}

func (m *BIObjectMeta) Category() int {
	return m.category
}

func (m *BIObjectMeta) ContentType() string {
	return m.contentType
}

func (m *BIObjectMeta) ETag() string {
	return m.etag
}

func (m *BIObjectMeta) MTime() string {
	return m.mtime
}

func (m *BIObjectMeta) Owner() string {
	return m.owner
}

func (m *BIObjectMeta) OwnerDisplayName() string {
	return m.ownerDisplayName
}

func (m *BIObjectMeta) Size() int {
	return m.size
}

func (m *BIObjectMeta) StorageClass() string {
	return m.storageClass
}

func (m *BIObjectMeta) UserData() string {
	return m.userData
}
