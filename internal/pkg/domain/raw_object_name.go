package domain

type RawObjectName struct {
	kind  string
	value string
}

func NewOLHRawObjectName(marker, objectName string) *RawObjectName {
	return &RawObjectName{
		kind:  "olh",
		value: marker + "_" + objectName,
	}
}

func NewVersionRawObjectName(marker, version, objectName string) *RawObjectName {
	return &RawObjectName{
		kind:  "version",
		value: marker + "__:" + version + "_" + objectName,
	}
}

func (n *RawObjectName) Kind() string {
	return n.kind
}

func (n *RawObjectName) Value() string {
	return n.value
}
