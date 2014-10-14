package bagman

import (
	"github.com/op/go-logging"
)


// WorkReader includes some basic components used by processes
// that read information from external sources to queue up work
// in NSQ.
type WorkReader struct {
	Config        Config
	MessageLog    *logging.Logger
	FluctusClient *FluctusClient
}
