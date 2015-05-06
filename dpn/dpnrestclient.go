// Package client provides a client for the DPN REST API.
package dpn

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/op/go-logging"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/cookiejar"
	"strings"
)

// Don't log error messages longer than this
const MAX_ERR_MSG_SIZE = 2048

type DPNRestClient struct {
	hostUrl      string
	apiVersion   string
	apiKey       string
	httpClient   *http.Client
	transport    *http.Transport
	logger       *logging.Logger
	institutions map[string]string
}

// Creates a new DPN REST client.
func NewDPNRestClient(hostUrl, apiVersion, apiKey string, logger *logging.Logger) (*DPNRestClient, error) {
	cookieJar, err := cookiejar.New(nil)
	if err != nil {
		return nil, fmt.Errorf("Can't create cookie jar for DPN REST client: %v", err)
	}
	transport := &http.Transport{
		MaxIdleConnsPerHost: 8,
		DisableKeepAlives:   false,
	}
	httpClient := &http.Client{Jar: cookieJar, Transport: transport}
	return &DPNRestClient{hostUrl, apiVersion, apiKey, httpClient, transport, logger, nil}, nil
}


// BuildUrl combines the host and protocol in client.hostUrl with
// relativeUrl to create an absolute URL. For example, if client.hostUrl
// is "http://localhost:3456", then client.BuildUrl("/path/to/action.json")
// would return "http://localhost:3456/path/to/action.json".
func (client *DPNRestClient) BuildUrl(relativeUrl string) string {
	return client.hostUrl + relativeUrl
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
	req.Header.Add("Authorization", fmt.Sprintf("token %s", client.apiKey))
	req.Header.Add("Connection", "Keep-Alive")
	return req, nil
}

func (client *DPNRestClient) DPNBagGet(identifier string) (*DPNBag, error) {
	objUrl := client.BuildUrl(fmt.Sprintf("/%s/bag/%s/", client.apiVersion, identifier))
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

func (client *DPNRestClient) DPNBagCreate(bag *DPNBag) (*DPNBag, error) {
	return client.dpnBagSave(bag, "POST")
}

func (client *DPNRestClient) DPNBagUpdate(bag *DPNBag) (*DPNBag, error) {
	return client.dpnBagSave(bag, "PUT")
}

func (client *DPNRestClient) dpnBagSave(bag *DPNBag, method string) (*DPNBag, error) {
	// POST/Create
	objUrl := client.BuildUrl(fmt.Sprintf("/%s/bag/", client.apiVersion))
	expectedResponseCode := 201
	if method == "PUT" {
		// PUT/Update
		objUrl = client.BuildUrl(fmt.Sprintf("/%s/bag/%s/", client.apiVersion, bag.UUID))
		expectedResponseCode = 200
	}
	client.logger.Debug("%sing new bag to DPN REST service: %s", method, objUrl)
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
		error := fmt.Errorf("%s to %s returned status code %d", method, objUrl, response.StatusCode)
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
	objUrl := client.BuildUrl(fmt.Sprintf("/%s/replicate/%s/", client.apiVersion, identifier))
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

func (client *DPNRestClient) RestoreTransferGet(identifier string) (*DPNRestoreTransfer, error) {
	// /api-v1/restore/aptrust-64/
	objUrl := client.BuildUrl(fmt.Sprintf("/%s/restore/%s/", client.apiVersion, identifier))
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

func (client *DPNRestClient) buildAndLogError(body []byte, formatString string, args ...interface{}) (err error) {
	if len(body) < MAX_ERR_MSG_SIZE {
		formatString += " Response body: %s"
		args = append(args, string(body))
	}
	err = fmt.Errorf(formatString, args...)
	client.logger.Error(err.Error())
	return err
}

func (client *DPNRestClient) formatJsonError(callerName string, body []byte, err error) (error) {
	json := strings.Replace(string(body), "\n", " ", -1)
	return fmt.Errorf("%s: Error parsing JSON response: %v -- JSON response: %s", err, json)
}
