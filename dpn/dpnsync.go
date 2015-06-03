package dpn

import (
	"fmt"
	"github.com/APTrust/bagman/bagman"
	"github.com/op/go-logging"
)

type DPNSync struct {
	LocalClient    *DPNRestClient
	RemoteClients  map[string]*DPNRestClient
	Logger         *logging.Logger
	Config         *DPNConfig
}

func NewDPNSync(config *DPNConfig) (*DPNSync, error) {
	logger := initLogger(config)
	localClient, err := NewDPNRestClient(
		config.RestClient.LocalServiceURL,
		config.RestClient.LocalAPIRoot,
		config.RestClient.LocalAuthToken,
		logger)
	if err != nil {
		return nil, fmt.Errorf("Error creating local DPN REST client: %v", err)
	}
	remoteClients, err := initRemoteClients(localClient, config, logger)
	if err != nil {
		return nil, fmt.Errorf("Error creating remote DPN REST client: %v", err)
	}
	sync := DPNSync{
		LocalClient: localClient,
		RemoteClients: remoteClients,
		Logger: logger,
		Config: config,
	}
	return &sync, nil
}

func initLogger(config *DPNConfig) (*logging.Logger) {
	// bagman has a nice InitLogger function, but it
	// needs a bagman.Config object, which does a lot
	// of handy internal work. Create a throw-away
	// bagman config so we can get our logger.
	bagmanConfig := bagman.Config{
		LogDirectory: config.LogDirectory,
		LogLevel: config.LogLevel,
		LogToStderr: config.LogToStderr,
	}
	return bagman.InitLogger(bagmanConfig)
}

func initRemoteClients(localClient *DPNRestClient, config *DPNConfig, logger *logging.Logger) (map[string]*DPNRestClient, error) {
	remoteClients := make(map[string]*DPNRestClient)
	for namespace, _ := range config.RemoteNodeTokens {
		remoteClient, err := localClient.GetRemoteClient(namespace, config, logger)
		if err != nil {
			return nil, fmt.Errorf("Error creating remote client for node %s: %v", namespace, err)
		}
		remoteClients[namespace] = remoteClient
	}
	return remoteClients, nil
}

//
// Get all nodes
// Get bags
// Sync bags to local
// Get replication transfers
// Sync replication to local
// Get restore transfers
// Sync restore to local
// Update last sync time for each node
