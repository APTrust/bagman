package bagman_test

import (
	"github.com/APTrust/bagman/bagman"
	"testing"
)

func TestEventTypeValid(t *testing.T) {
	for _, eventType := range bagman.EventTypes {
		premisEvent := &bagman.PremisEvent{
			EventType: eventType,
		}
		if premisEvent.EventTypeValid() == false {
			t.Errorf("EventType '%s' should be valid", eventType)
		}
	}
	premisEvent := &bagman.PremisEvent{
		EventType: "pub_crawl",
	}
	if premisEvent.EventTypeValid() == true {
		t.Errorf("EventType 'pub_crawl' should not be valid")
	}
}
