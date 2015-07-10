package dpn

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/op/go-logging"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"regexp"
	"strings"
	"time"
)

// Don't log error messages longer than this
const MAX_ERR_MSG_SIZE = 2048

// DPNRestClient is a client for the DPN REST API.
type DPNRestClient struct {
	HostUrl      string
	APIVersion   string
	APIKey       string
	httpClient   *http.Client
	transport    *http.Transport
	logger       *logging.Logger
	institutions map[string]string
}

type NodeListResult struct {
	Count       int32                      `json:count`
	Next        *string                    `json:next`
	Previous    *string                    `json:previous`
	Results     []*DPNNode                 `json:results`
}

// BagListResult is what the REST service returns when
// we ask for a list of bags.
type BagListResult struct {
	Count       int32                      `json:count`
	Next        *string                    `json:next`
	Previous    *string                    `json:previous`
	Results     []*DPNBag                  `json:results`
}

// ReplicationListResult is what the REST service returns when
// we ask for a list of transfer requests.
type ReplicationListResult struct {
	Count       int32                     `json:count`
	Next        *string                   `json:next`
	Previous    *string                   `json:previous`
	Results     []*DPNReplicationTransfer `json:results`
}

// RestoreListResult is what the REST service returns when
// we ask for a list of restore requests.
type RestoreListResult struct {
	Count       int32                     `json:count`
	Next        *string                   `json:next`
	Previous    *string                   `json:previous`
	Results     []*DPNRestoreTransfer     `json:results`
}


// Creates a new DPN REST client.
func NewDPNRestClient(hostUrl, apiVersion, apiKey string, acceptInvalidSSLCerts bool, logger *logging.Logger) (*DPNRestClient, error) {
	cookieJar, err := cookiejar.New(nil)
	if err != nil {
		return nil, fmt.Errorf("Can't create cookie jar for DPN REST client: %v", err)
	}
	transport := &http.Transport{
		MaxIdleConnsPerHost: 8,
		DisableKeepAlives:   false,
		Dial: (&net.Dialer{
			Timeout:   10 * time.Second,
			KeepAlive: 30 * time.Second,
		}).Dial,
		ResponseHeaderTimeout: 10 * time.Second,
		TLSHandshakeTimeout: 10 * time.Second,
	}
	if acceptInvalidSSLCerts {
		transport.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
	}
	httpClient := &http.Client{Jar: cookieJar, Transport: transport}
	// Trim trailing slashes from host url
	for strings.HasSuffix(hostUrl, "/") {
		hostUrl = hostUrl[:len(hostUrl)-1]
	}
	return &DPNRestClient{hostUrl, apiVersion, apiKey, httpClient, transport, logger, nil}, nil
}


// BuildUrl combines the host and protocol in client.HostUrl with
// relativeUrl to create an absolute URL. For example, if client.HostUrl
// is "http://localhost:3456", then client.BuildUrl("/path/to/action.json")
// would return "http://localhost:3456/path/to/action.json".
func (client *DPNRestClient) BuildUrl(relativeUrl string, queryParams *url.Values) string {
	fullUrl := client.HostUrl + relativeUrl
	if queryParams != nil {
		fullUrl = fmt.Sprintf("%s?%s", fullUrl, queryParams.Encode())
	}
	return fullUrl
}

// newJsonGet returns a new request with headers indicating
// JSON request and response formats.
func (client *DPNRestClient) NewJsonRequest(method, targetUrl string, body io.Reader) (*http.Request, error) {
	req, err := http.NewRequest(method, targetUrl, body)
	if err != nil {
		return nil, err
	}
	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("Accept", "application/json")
	req.Header.Add("Authorization", fmt.Sprintf("token %s", client.APIKey))
	req.Header.Add("Connection", "Keep-Alive")
	return req, nil
}

func (client *DPNRestClient) DPNNodeGet(identifier string) (*DPNNode, error) {
	relativeUrl := fmt.Sprintf("/%s/node/%s/", client.APIVersion, identifier)
	objUrl := client.BuildUrl(relativeUrl, nil)
	client.logger.Debug("Requesting node from DPN REST service: %s", objUrl)
	request, err := client.NewJsonRequest("GET", objUrl, nil)
	if err != nil {
		return nil, err
	}
	body, response, err := client.doRequest(request)
	if err != nil {
		return nil, err
	}

	// 404 for object not found
	if response.StatusCode != 200 {
		error := fmt.Errorf("DPNNodeGet expected status 200 but got %d. URL: %s", response.StatusCode, objUrl)
		client.buildAndLogError(body, error.Error())
		return nil, error
	}

	// HACK! Get rid of this when Golang fixes the JSON null date problem!
	body = HackNullDates(body)

	// Build and return the data structure
	obj := &DPNNode{}
	err = json.Unmarshal(body, obj)
	if err != nil {
		return nil, client.formatJsonError(objUrl, body, err)
	}
	return obj, nil
}

func (client *DPNRestClient) DPNNodeListGet(queryParams *url.Values) (*NodeListResult, error) {
	relativeUrl := fmt.Sprintf("/%s/node/", client.APIVersion)
	objUrl := client.BuildUrl(relativeUrl, queryParams)
	client.logger.Debug("Requesting node list from DPN REST service: %s", objUrl)
	request, err := client.NewJsonRequest("GET", objUrl, nil)
	if err != nil {
		return nil, err
	}
	body, response, err := client.doRequest(request)
	if err != nil {
		return nil, err
	}

	if response.StatusCode != 200 {
		error := fmt.Errorf("DPNNodeListGet expected status 200 but got %d. URL: %s", response.StatusCode, objUrl)
		client.buildAndLogError(body, error.Error())
		return nil, error
	}

	// HACK! Get rid of this when Golang fixes the JSON null date problem!
	body = HackNullDates(body)

	// Build and return the data structure
	result := &NodeListResult{}
	err = json.Unmarshal(body, result)
	if err != nil {
		return nil, client.formatJsonError(objUrl, body, err)
	}
	return result, nil
}


// DPNNodeUpdate updates a DPN Node record. You can update node
// records only if you are the admin on the server where you're
// updating the record. Though this method lets you update any
// attributes related to the node, you should update only the
// LastPullDate attribute through this client. Use the web admin
// interface to perform more substantive node updates.
func (client *DPNRestClient) DPNNodeUpdate(node *DPNNode) (*DPNNode, error) {
	relativeUrl := fmt.Sprintf("/%s/node/%s/", client.APIVersion, node.Namespace)
	objUrl := client.BuildUrl(relativeUrl, nil)
	expectedResponseCode := 200
	client.logger.Debug("Updating Node %s to DPN REST service: %s", node.Namespace, objUrl)
	postData, err := json.Marshal(node)
	if err != nil {
		return nil, err
	}
	req, err := client.NewJsonRequest("PUT", objUrl, bytes.NewBuffer(postData))
	if err != nil {
		return nil, err
	}
	body, response, err := client.doRequest(req)
	if err != nil {
		return nil, err
	}
	if response.StatusCode != expectedResponseCode {
		error := fmt.Errorf("PUT to %s returned status code %d", objUrl, response.StatusCode)
		client.buildAndLogError(body, error.Error())
		fmt.Println(string(body))
		return nil, error
	}

	// HACK! Get rid of this when Golang fixes the JSON null date problem!
	body = HackNullDates(body)

	returnedNode := DPNNode{}
	err = json.Unmarshal(body, &returnedNode)
	if err != nil {
		error := fmt.Errorf("Could not parse JSON response from  %s", objUrl)
		client.buildAndLogError(body, error.Error())
		return nil, error
	}
	return &returnedNode, nil
}


func (client *DPNRestClient) DPNBagGet(identifier string) (*DPNBag, error) {
	relativeUrl := fmt.Sprintf("/%s/bag/%s/", client.APIVersion, identifier)
	objUrl := client.BuildUrl(relativeUrl, nil)
	client.logger.Debug("Requesting bag from DPN REST service: %s", objUrl)
	request, err := client.NewJsonRequest("GET", objUrl, nil)
	if err != nil {
		return nil, err
	}
	body, response, err := client.doRequest(request)
	if err != nil {
		return nil, err
	}

	// 404 for object not found
	if response.StatusCode != 200 {
		error := fmt.Errorf("DPNBagGet expected status 200 but got %d. URL: %s", response.StatusCode, objUrl)
		client.buildAndLogError(body, error.Error())
		return nil, error
	}

	// Build and return the data structure
	obj := &DPNBag{}
	err = json.Unmarshal(body, obj)
	if err != nil {
		return nil, client.formatJsonError(objUrl, body, err)
	}
	return obj, nil
}

func (client *DPNRestClient) DPNBagListGet(queryParams *url.Values) (*BagListResult, error) {
	relativeUrl := fmt.Sprintf("/%s/bag/", client.APIVersion)
	objUrl := client.BuildUrl(relativeUrl, queryParams)
	client.logger.Debug("Requesting bag list from DPN REST service: %s", objUrl)
	request, err := client.NewJsonRequest("GET", objUrl, nil)
	if err != nil {
		return nil, err
	}
	body, response, err := client.doRequest(request)
	if err != nil {
		return nil, err
	}

	if response.StatusCode != 200 {
		error := fmt.Errorf("DPNBagListGet expected status 200 but got %d. URL: %s",
			response.StatusCode, objUrl)
		client.buildAndLogError(body, error.Error())
		return nil, error
	}

	// Build and return the data structure
	result := &BagListResult{}
	err = json.Unmarshal(body, result)
	if err != nil {
		return nil, client.formatJsonError(objUrl, body, err)
	}
	return result, nil
}


func (client *DPNRestClient) DPNBagCreate(bag *DPNBag) (*DPNBag, error) {
	return client.dpnBagSave(bag, "POST")
}

func (client *DPNRestClient) DPNBagUpdate(bag *DPNBag) (*DPNBag, error) {
	return client.dpnBagSave(bag, "PUT")
}

func (client *DPNRestClient) dpnBagSave(bag *DPNBag, method string) (*DPNBag, error) {
	// POST/Create
	relativeUrl := fmt.Sprintf("/%s/bag/", client.APIVersion)
	objUrl := client.BuildUrl(relativeUrl, nil)
	expectedResponseCode := 201
	if method == "PUT" {
		// PUT/Update
		relativeUrl = fmt.Sprintf("/%s/bag/%s/", client.APIVersion, bag.UUID)
		objUrl = client.BuildUrl(relativeUrl, nil)
		expectedResponseCode = 200
	}
	client.logger.Debug("%sing bag to DPN REST service: %s", method, objUrl)
	postData, err := json.Marshal(bag)
	if err != nil {
		return nil, err
	}
	req, err := client.NewJsonRequest(method, objUrl, bytes.NewBuffer(postData))
	if err != nil {
		return nil, err
	}
	body, response, err := client.doRequest(req)
	if err != nil {
		return nil, err
	}
	if response.StatusCode != expectedResponseCode {
		error := fmt.Errorf("%s to %s returned status code %d. Post data: %v",
			method, objUrl, response.StatusCode, string(postData))
		client.buildAndLogError(body, error.Error())
		fmt.Println(string(body))
		return nil, error
	}
	returnedBag := DPNBag{}
	err = json.Unmarshal(body, &returnedBag)
	if err != nil {
		error := fmt.Errorf("Could not parse JSON response from  %s", objUrl)
		client.buildAndLogError(body, error.Error())
		return nil, error
	}
	return &returnedBag, nil
}

func (client *DPNRestClient) ReplicationTransferGet(identifier string) (*DPNReplicationTransfer, error) {
	// /api-v1/replicate/aptrust-999999/
	relativeUrl := fmt.Sprintf("/%s/replicate/%s/", client.APIVersion, identifier)
	objUrl := client.BuildUrl(relativeUrl, nil)
	client.logger.Debug("Requesting replication xfer record from DPN REST service: %s", objUrl)
	request, err := client.NewJsonRequest("GET", objUrl, nil)
	if err != nil {
		return nil, err
	}
	body, response, err := client.doRequest(request)
	if err != nil {
		return nil, err
	}

	// 404 for object not found
	if response.StatusCode != 200 {
		error := fmt.Errorf("ReplicationTransferGet expected status 200 but got %d. URL: %s",
			response.StatusCode, objUrl)
		client.buildAndLogError(body, error.Error())
		return nil, error
	}

	// Build and return the data structure
	obj := &DPNReplicationTransfer{}
	err = json.Unmarshal(body, obj)
	if err != nil {
		return nil, client.formatJsonError(objUrl, body, err)
	}
	return obj, nil
}

func (client *DPNRestClient) DPNReplicationListGet(queryParams *url.Values) (*ReplicationListResult, error) {
	relativeUrl := fmt.Sprintf("/%s/replicate/", client.APIVersion)
	objUrl := client.BuildUrl(relativeUrl, queryParams)
	client.logger.Debug("Requesting replication list from DPN REST service: %s", objUrl)
	request, err := client.NewJsonRequest("GET", objUrl, nil)
	if err != nil {
		return nil, err
	}
	body, response, err := client.doRequest(request)
	if err != nil {
		return nil, err
	}

	if response.StatusCode != 200 {
		error := fmt.Errorf("DPNReplicationListGet expected status 200 but got %d. URL: %s",
			response.StatusCode, objUrl)
		client.buildAndLogError(body, error.Error())
		return nil, error
	}

	// Build and return the data structure
	result := &ReplicationListResult{}
	err = json.Unmarshal(body, result)
	if err != nil {
		return nil, client.formatJsonError(objUrl, body, err)
	}
	return result, nil
}


func (client *DPNRestClient) ReplicationTransferCreate(xfer *DPNReplicationTransfer) (*DPNReplicationTransfer, error) {
	return client.replicationTransferSave(xfer, "POST")
}

func (client *DPNRestClient) ReplicationTransferUpdate(xfer *DPNReplicationTransfer) (*DPNReplicationTransfer, error) {
	return client.replicationTransferSave(xfer, "PUT")
}

func (client *DPNRestClient) replicationTransferSave(xfer *DPNReplicationTransfer, method string) (*DPNReplicationTransfer, error) {
	// POST/Create
	relativeUrl := fmt.Sprintf("/%s/replicate/", client.APIVersion)
	objUrl := client.BuildUrl(relativeUrl, nil)
	expectedResponseCode := 201
	if method == "PUT" {
		// PUT/Update
		relativeUrl = fmt.Sprintf("/%s/replicate/%s/", client.APIVersion, xfer.ReplicationId)
		objUrl = client.BuildUrl(relativeUrl, nil)
		expectedResponseCode = 200
	}
	client.logger.Debug("%sing replication transfer to DPN REST service: %s", method, objUrl)
	postData, err := json.Marshal(xfer)
	if err != nil {
		return nil, err
	}
	req, err := client.NewJsonRequest(method, objUrl, bytes.NewBuffer(postData))
	if err != nil {
		return nil, err
	}
	body, response, err := client.doRequest(req)
	if err != nil {
		return nil, err
	}
	if response.StatusCode != expectedResponseCode {
		error := fmt.Errorf("%s to %s returned status code %d. Post data: %v",
			method, objUrl, response.StatusCode, string(postData))
		client.buildAndLogError(body, error.Error())
		fmt.Println(string(body))
		return nil, error
	}
	returnedXfer := DPNReplicationTransfer{}
	err = json.Unmarshal(body, &returnedXfer)
	if err != nil {
		error := fmt.Errorf("Could not parse JSON response from  %s", objUrl)
		client.buildAndLogError(body, error.Error())
		return nil, error
	}
	return &returnedXfer, nil
}

func (client *DPNRestClient) RestoreTransferGet(identifier string) (*DPNRestoreTransfer, error) {
	// /api-v1/restore/aptrust-64/
	relativeUrl := fmt.Sprintf("/%s/restore/%s/", client.APIVersion, identifier)
	objUrl := client.BuildUrl(relativeUrl, nil)
	client.logger.Debug("Requesting restore xfer record from DPN REST service: %s", objUrl)
	request, err := client.NewJsonRequest("GET", objUrl, nil)
	if err != nil {
		return nil, err
	}
	body, response, err := client.doRequest(request)
	if err != nil {
		return nil, err
	}

	// 404 for object not found
	if response.StatusCode != 200 {
		error := fmt.Errorf("RestoreTransferGet expected status 200 but got %d. URL: %s",
			response.StatusCode, objUrl)
		client.buildAndLogError(body, error.Error())
		return nil, error
	}

	// Build and return the data structure
	obj := &DPNRestoreTransfer{}
	err = json.Unmarshal(body, obj)
	if err != nil {
		return nil, client.formatJsonError(objUrl, body, err)
	}
	return obj, nil
}

func (client *DPNRestClient) DPNRestoreListGet(queryParams *url.Values) (*RestoreListResult, error) {
	relativeUrl := fmt.Sprintf("/%s/restore/", client.APIVersion)
	objUrl := client.BuildUrl(relativeUrl, queryParams)
	client.logger.Debug("Requesting restore list from DPN REST service: %s", objUrl)
	request, err := client.NewJsonRequest("GET", objUrl, nil)
	if err != nil {
		return nil, err
	}
	body, response, err := client.doRequest(request)
	if err != nil {
		return nil, err
	}

	if response.StatusCode != 200 {
		error := fmt.Errorf("DPNRestoreListGet expected status 200 but got %d. URL: %s",
			response.StatusCode, objUrl)
		client.buildAndLogError(body, error.Error())
		return nil, error
	}

	// Build and return the data structure
	result := &RestoreListResult{}
	err = json.Unmarshal(body, result)
	if err != nil {
		return nil, client.formatJsonError(objUrl, body, err)
	}
	return result, nil
}

func (client *DPNRestClient) RestoreTransferCreate(xfer *DPNRestoreTransfer) (*DPNRestoreTransfer, error) {
	return client.restoreTransferSave(xfer, "POST")
}

func (client *DPNRestClient) RestoreTransferUpdate(xfer *DPNRestoreTransfer) (*DPNRestoreTransfer, error) {
	return client.restoreTransferSave(xfer, "PUT")
}

func (client *DPNRestClient) restoreTransferSave(xfer *DPNRestoreTransfer, method string) (*DPNRestoreTransfer, error) {
	// POST/Create
	relativeUrl := fmt.Sprintf("/%s/restore/", client.APIVersion)
	objUrl := client.BuildUrl(relativeUrl, nil)
	expectedResponseCode := 201
	if method == "PUT" {
		// PUT/Update
		relativeUrl = fmt.Sprintf("/%s/restore/%s/", client.APIVersion, xfer.RestoreId)
		objUrl = client.BuildUrl(relativeUrl, nil)
		expectedResponseCode = 200
	}
	client.logger.Debug("%sing restore transfer to DPN REST service: %s", method, objUrl)
	postData, err := json.Marshal(xfer)
	if err != nil {
		return nil, err
	}
	req, err := client.NewJsonRequest(method, objUrl, bytes.NewBuffer(postData))
	if err != nil {
		return nil, err
	}
	body, response, err := client.doRequest(req)
	if err != nil {
		return nil, err
	}
	if response.StatusCode != expectedResponseCode {
		error := fmt.Errorf("%s to %s returned status code %d. Post data: %v",
			method, objUrl, response.StatusCode, string(postData))
		client.buildAndLogError(body, error.Error())
		fmt.Println(string(body))
		return nil, error
	}

	returnedXfer := DPNRestoreTransfer{}
	err = json.Unmarshal(body, &returnedXfer)
	if err != nil {
		error := fmt.Errorf("Could not parse JSON response from  %s", objUrl)
		client.buildAndLogError(body, error.Error())
		return nil, error
	}
	return &returnedXfer, nil
}


// Returns a DPN REST client that can talk to a remote node.
// This function has to connect to out local DPN node to get
// information about the remote node. It returns a new client
// that can connect to the remote node with the correct URL
// and API key. We use this function to get a client that can
// update a replication request or a restore request on the
// originating node.
func (client *DPNRestClient) GetRemoteClient(remoteNodeNamespace string, dpnConfig *DPNConfig, logger *logging.Logger) (*DPNRestClient, error) {
	remoteNode, err := client.DPNNodeGet(remoteNodeNamespace)
	if err != nil {
		detailedError := fmt.Errorf("Error retrieving node record for '%s' "+
			"from local DPN REST service: %v", remoteNodeNamespace, err)
		return nil, detailedError
	}

	authToken := dpnConfig.RemoteNodeTokens[remoteNode.Namespace]
	if authToken == "" {
		detailedError := fmt.Errorf("Cannot get auth token for node %s", remoteNode.Namespace)
		return nil, detailedError
	}
	apiRoot := remoteNode.APIRoot
	if dpnConfig.RemoteNodeURLs != nil && dpnConfig.RemoteNodeURLs[remoteNodeNamespace] != "" {
		logger.Debug("Overriding DPN REST client URL for node %s " +
			"because DPNConfig.RemoteNodeURLs " +
			"says the URL for that node should be %s",
			remoteNodeNamespace,
			dpnConfig.RemoteNodeURLs[remoteNodeNamespace])
		apiRoot = dpnConfig.RemoteNodeURLs[remoteNodeNamespace]
	}
	remoteRESTClient, err := NewDPNRestClient(
		apiRoot,
		dpnConfig.RestClient.LocalAPIRoot, // All nodes should be on same version
		authToken,
		dpnConfig.AcceptInvalidSSLCerts,
		logger)
	if err != nil {
		detailedError := fmt.Errorf("Could not create REST client for remote node %s: %v",
			remoteNode.Namespace, err)
		return nil, detailedError
	}
	return remoteRESTClient, nil
}

// Reads the response body and returns a byte slice.
// You must read and close the response body, or the
// TCP connection will remain open for as long as
// our application runs.
func readResponse(body io.ReadCloser) (data []byte, err error) {
	if body != nil {
		data, err = ioutil.ReadAll(body)
		body.Close()
	}
	return data, err
}

func (client *DPNRestClient) doRequest(request *http.Request) (data []byte, response *http.Response, err error) {
	response, err = client.httpClient.Do(request)
	if err != nil {
		return nil, nil, err
	}
	data, err = readResponse(response.Body)
	if err != nil {
		return nil, response, err
	}
	return data, response, err
}

func (client *DPNRestClient) buildAndLogError(body []byte, errStr string) (err error) {
	if len(body) < MAX_ERR_MSG_SIZE {
		errStr += fmt.Sprintf(" Response body: %s", string(body))
	}
	err = errors.New(errStr)
	client.logger.Error(strings.Replace(err.Error(), "%", "%%", -1))
	return err
}

func (client *DPNRestClient) formatJsonError(callerName string, body []byte, err error) (error) {
	json := strings.Replace(string(body), "\n", " ", -1)
	return fmt.Errorf("%s: Error parsing JSON response: %v -- JSON response: %s", err, json)
}

// This hack works around the JSON decoding bug in Golang's core
// time and json libraries. The bug is described here:
// https://go-review.googlesource.com/#/c/9376/
// We could do a "proper" work-around by changing all our structs
// to use pointers to time.Time instead of time.Time values, but then
// we have to check for nil in many places. There's already a patch
// in to fix this bug in the next release of Golang, so
// I'd rather have this hack for now and remove it when the next
// version of Golang comes out. That's better than changing to pointers,
// checking for nil in a hundred places, and then reverting ALL THAT when
// the bug is fixed. In practice the only null dates that should ever come
// out of our REST services are Node.LastPullDate, and that should only
// happen on the first day a new node is up and runnning. We just have
// to set these back to a reasonably old timestamp so we can ask the
// node for all items updated since that time. "Reasonably old" is
// anything before about June 1, 2015.
func HackNullDates(jsonBytes []byte)([]byte) {
	// Problem fixed with regex == two problems
	dummyDate := "\"last_pull_date\":\"1980-01-01T00:00:00Z\""
	re := regexp.MustCompile("\"last_pull_date\":\\s*null")
	return re.ReplaceAll(jsonBytes, []byte(dummyDate))
}
