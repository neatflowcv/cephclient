package domain

import (
	"strings"
	"unicode/utf8"
)

const (
	biIndexHexBase     = 16
	biIndexHexLowerNib = 0x0f
	biIndexDecimalBase = 10
	biIndexRuneError   = 0x80
)

type BIIndex struct {
	raw string
}

func NewBIIndex(raw string) *BIIndex {
	return &BIIndex{raw: raw}
}

func (i *BIIndex) Raw() string {
	return i.raw
}

func (i *BIIndex) Escaped() string {
	var builder strings.Builder

	for _, currentRune := range i.raw {
		if currentRune == utf8.RuneError {
			builder.WriteString(`\x`)
			builder.WriteByte(hexDigit(biIndexRuneError / biIndexHexBase))
			builder.WriteByte(hexDigit(biIndexRuneError & biIndexHexLowerNib))

			continue
		}

		for _, currentByte := range []byte(string(currentRune)) {
			switch {
			case currentByte >= 0x20 && currentByte <= 0x7e && currentByte != '\\':
				builder.WriteByte(currentByte)
			case currentByte == '\\':
				builder.WriteString("\\\\")
			default:
				builder.WriteString(`\x`)
				builder.WriteByte(hexDigit(currentByte / biIndexHexBase))
				builder.WriteByte(hexDigit(currentByte & biIndexHexLowerNib))
			}
		}
	}

	return builder.String()
}

func hexDigit(value byte) byte {
	if value < biIndexDecimalBase {
		return '0' + value
	}

	return 'a' + (value - biIndexDecimalBase)
}
