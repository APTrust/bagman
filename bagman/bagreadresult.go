package bagman

import (
	"strings"
)

// BagReadResult contains data describing the result of
// processing a single bag. If there were any processing
// errors, this structure should tell us exactly what
// happened and where.
type BagReadResult struct {
	Path           string
	Files          []string
	ErrorMessage   string
	Tags           []Tag
	ChecksumErrors []error
}

// TagValue returns the value of the tag with the specified label.
func (result *BagReadResult) TagValue(tagLabel string) (tagValue string) {
	lcTagLabel := strings.ToLower(tagLabel)
	for _, tag := range result.Tags {
		if strings.ToLower(tag.Label) == lcTagLabel {
			tagValue = tag.Value
			break
		}
	}
	return tagValue
}
