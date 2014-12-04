// +build !partners cgo

// This requires an external C library that our partners won't have,
// so this file is not compiled when the flag -tags=partners
package bagman

import (
	"fmt"
	"github.com/rakyll/magicmime"
	"regexp"
)

// magicMime is the MimeMagic database. We want
// just one copy of this open at a time.
var magicMime *magicmime.Magic

var validMimeType = regexp.MustCompile(`^\w+/\w+$`)

func GuessMimeType(absPath string) (mimeType string, err error) {
	// Open the Mime Magic DB only once.
	if magicMime == nil {
		magicMime, err = magicmime.New()
		if err != nil {
			return "", fmt.Errorf("Error opening MimeMagic database: %v", err)
		}
	}

	// Get the mime type of the file. In some cases, MagicMime
	// returns an empty string, and in rare cases (about 1 in 10000),
	// it returns unprintable characters. These are not valid mime
	// types and cause ingest to fail. So we default to the safe
	// application/binary and then set the MimeType only if
	// MagicMime returned something that looks legit.
	mimeType = "application/binary"
	guessedType, _ := magicMime.TypeByFile(absPath)
	if guessedType != "" && validMimeType.MatchString(guessedType) {
		mimeType = guessedType
	}
	return mimeType, nil
}
