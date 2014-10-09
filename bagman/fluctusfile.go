package bagman

import (
	"fmt"
	"encoding/json"
	"strings"
	"time"
)


/*
FluctusFile contains information about a file that makes up
part (or all) of an IntellectualObject.

IntellectualObject is the object to which the file belongs.

Format is typically a mime-type, such as "application/xml",
that describes the file format.

URI describes the location of the object (in APTrust?).

Size is the size of the object, in bytes.

Created is the date and time at which the object was created
(in APTrust, or at the institution that owns it?).

Modified is the data and time at which the object was last
modified (in APTrust, or at the institution that owns it?).

Created and Modified should be ISO8601 DateTime strings,
such as:

1994-11-05T08:15:30-05:00     (Local Time)
1994-11-05T08:15:30Z          (UTC)
*/
type FluctusFile struct {
	Id                 string               `json:"id"`
	Identifier         string               `json:"identifier"`
	Format             string               `json:"file_format"`
	URI                string               `json:"uri"`
	Size               int64                `json:"size"`
	Created            time.Time            `json:"created"`
	Modified           time.Time            `json:"modified"`
	ChecksumAttributes []*ChecksumAttribute `json:"checksum"`
	Events             []*PremisEvent       `json:"premisEvents"`
}

// Serializes a version of FluctusFile that Fluctus will accept as post/put input.
func (gf *FluctusFile) SerializeForFluctus() ([]byte, error) {
	return json.Marshal(map[string]interface{}{
		"identifier":          gf.Identifier,
		"file_format":         gf.Format,
		"uri":                 gf.URI,
		"size":                gf.Size,
		"created":             gf.Created,
		"modified":            gf.Modified,
		"checksum_attributes": gf.ChecksumAttributes,
	})
}

// Returns the original path of the file within the original bag.
// This is just the identifier minus the institution id and bag name.
// For example, if the identifier is "uc.edu/cin.675812/data/object.properties",
// this returns "data/object.properties"
func (gf *FluctusFile) OriginalPath() (string, error) {
	parts := strings.SplitN(gf.Identifier, "/data/", 2)
	if len(parts) < 2 {
		return "", fmt.Errorf("FluctusFile identifier '%s' is not valid", gf.Identifier)
	}
	return fmt.Sprintf("data/%s", parts[1]), nil
}

// Returns the name of the original bag.
func (gf *FluctusFile) BagName() (string, error) {
	parts := strings.Split(gf.Identifier, "/")
	if len(parts) < 2 {
		return "", fmt.Errorf("FluctusFile identifier '%s' is not valid", gf.Identifier)
	}
	return parts[1], nil
}

// Returns the name of the institution that owns this file.
func (gf *FluctusFile) InstitutionId() (string, error) {
	parts := strings.Split(gf.Identifier, "/")
	if len(parts) < 2 {
		return "", fmt.Errorf("FluctusFile identifier '%s' is not valid", gf.Identifier)
	}
	return parts[0], nil
}

// Returns the checksum digest for the given algorithm for this file.
func (gf *FluctusFile) GetChecksum(algorithm string) (*ChecksumAttribute) {
	for _, cs := range gf.ChecksumAttributes {
		if cs.Algorithm == algorithm {
			return cs
		}
	}
	return nil
}

// Returns the name of this file in the preservation storage bucket
// (that should be a UUID), or an error if the FluctusFile does not
// have a valid preservation storage URL.
func (gf *FluctusFile) PreservationStorageFileName() (string, error) {
	if strings.Index(gf.URI, "/") < 0 {
		return "", fmt.Errorf("Cannot get preservation storage file name because FluctusFile has an invalid URI")
	}
	parts := strings.Split(gf.URI, "/")
	return parts[len(parts) - 1], nil
}
