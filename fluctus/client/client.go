// Package client provides a client for the fluctus REST API.
package client

import (
	"net/http"
	"net/url"
	"net/http/cookiejar"
	"io"
	"io/ioutil"
	"encoding/json"
//	"regexp"
	"fmt"
	"bytes"
	"log"
	"time"
	"github.com/APTrust/bagman"
	"github.com/APTrust/bagman/fluctus/models"
)

type Client struct {
	hostUrl        string
	apiUser        string
	apiKey         string
	httpClient     *http.Client
	transport      *http.Transport
	logger         *log.Logger
	requestCount   int
}

// Creates a new fluctus client. Param hostUrl should come from
// the config.json file.
func New(hostUrl, apiUser, apiKey string, logger *log.Logger) (*Client, error) {
	// see security warning on nil PublicSuffixList here:
	// http://gotour.golang.org/src/pkg/net/http/cookiejar/jar.go?s=1011:1492#L24
	cookieJar, err := cookiejar.New(nil)
	if err != nil {
		return nil, fmt.Errorf("Can't create cookie jar for HTTP client: %v", err)
	}
	transport := &http.Transport{ MaxIdleConnsPerHost: 12 }
	httpClient := &http.Client{ Jar: cookieJar, Transport: transport }
	return &Client{hostUrl, apiUser, apiKey, httpClient, transport, logger, 0}, nil
}


// BuildUrl combines the host and protocol in client.hostUrl with
// relativeUrl to create an absolute URL. For example, if client.hostUrl
// is "http://localhost:3456", then client.BuildUrl("/path/to/action.json")
// would return "http://localhost:3456/path/to/action.json".
func (client *Client) BuildUrl (relativeUrl string) (absoluteUrl *url.URL) {
	absoluteUrl, err := url.Parse(client.hostUrl + relativeUrl)
	if err != nil {
		// TODO: Check validity of Fluctus url on app init,
		// so we don't have to panic here.
		panic(fmt.Sprintf("Can't parse URL '%s': %v", absoluteUrl, err))
	}
	return absoluteUrl
}


// newJsonGet returns a new request with headers indicating
// JSON request and response formats.
func (client *Client) NewJsonRequest(method, url string, body io.Reader) (*http.Request, error) {
	req, err := http.NewRequest(method, url, body)
	if err != nil {
		return nil, err
	}
	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("Accept", "application/json")
	req.Header.Add("X-Fluctus-API-User", client.apiUser)
	req.Header.Add("X-Fluctus-API-Key", client.apiKey)
	return req, nil
}


// GetBagStatus returns the status of a bag from a prior round of processing.
// This function will return nil if Fluctus has no record of this bag.
func (client *Client) GetBagStatus(etag, name string, bag_date time.Time) (status *bagman.ProcessStatus, err error) {
	url := client.BuildUrl(fmt.Sprintf("/itemresults/%s/%s/%s", etag, name, bag_date.Format(time.RFC3339)))
	req, err := client.NewJsonRequest("GET", url.String(), nil)
	if err != nil {
		return nil, err
	}
	status, err = client.doStatusRequest(req, 200)
	return status, err
}

// UpdateBagStatus sends a message to Fluctus describing whether bag
// processing succeeded or failed. If it failed, the ProcessStatus
// object includes some details of what went wrong.
func (client *Client) UpdateBagStatus(status *bagman.ProcessStatus) (err error) {
	relativeUrl := "/itemresults"
	httpMethod := "POST"
	if status.Id > 0 {
		relativeUrl = fmt.Sprintf("/itemresults/%s/%s/%s",
			status.ETag, status.Name, status.BagDate.Format(time.RFC3339))
		httpMethod = "PUT"
	}
	url := client.BuildUrl(relativeUrl)
	postData, err := status.SerializeForFluctus()
	if err != nil {
		return err
	}
	req, err := client.NewJsonRequest(httpMethod, url.String(), bytes.NewBuffer(postData))
	if err != nil {
		return err
	}
	status, err = client.doStatusRequest(req, 201)
	if err != nil {
		client.logger.Printf("[ERROR] JSON for failed Fluctus request: %s",
			string(postData))
	}
	return err
}

func (client *Client) doStatusRequest(request *http.Request, expectedStatus int) (status *bagman.ProcessStatus, err error) {
	// Trying to fix problem with too many connections in CLOSE_WAIT state.
	// If we don't forcibly close idle connections, they'll pile up
	// and the system fails with "too many open files" error.
	// The call to CloseIdleConnections will close some idle connections
	// that are in TIME_WAIT / keepalive, but we can pay that price.
	client.requestCount++
	if client.requestCount % 100 == 0 {
		// client.logger.Println("[INFO] Used to force-close connections here...")
		// client.logger.Println("[INFO] Fluctus client is forcibly closing idle HTTP connections")
		// client.transport.CloseIdleConnections()
	}

	response, err := client.httpClient.Do(request)
	if err != nil {
		return nil, err
	}

	// OK to return 404 on a status check. It just means the bag has not
	// been processed before.
	if response.StatusCode == 404 && request.Method == "GET" {
		return nil, nil
	}

	if response.StatusCode != expectedStatus {
		err = fmt.Errorf("Expected status code %d but got %d. URL: %s",
			expectedStatus, response.StatusCode, request.URL)
		return nil, err
	}
	defer response.Body.Close()
	body, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}

	// Build and return the data structure
	err = json.Unmarshal(body, &status)
	if err != nil {
		return nil, err
	}
	return status, nil
}

// Returns true/false indicating whether an IntellectualObject
// exists in Fluctus.
func (client *Client) IntellectualObjectGet (identifier string) (*models.IntellectualObject, error) {
	url := client.BuildUrl(fmt.Sprintf("/objects/%s", identifier))
	client.logger.Println("[INFO] Requesting IntellectualObject from fluctus:", url)
	request, err := client.NewJsonRequest("GET", url.String(), nil)
	if err != nil {
		fmt.Println(err)
		return nil, err
	}
	response, err := client.httpClient.Do(request)
	if err != nil {
		fmt.Println(err)
		return nil, err
	}
	// 404 for object not found
	if response.StatusCode != 200 {
		return nil, nil
	}

	// Read the json response
	defer response.Body.Close()
	body, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}

	// Build and return the data structure
	obj := &models.IntellectualObject{}
	err = json.Unmarshal(body, obj)
	if err != nil {
		return nil, err
	}
	return obj, nil
}


// Saves an IntellectualObject to fluctus.
func (client *Client) IntellectualObjectSave (obj *models.IntellectualObject) (newObj *models.IntellectualObject, err error) {
	existingObj, err := client.IntellectualObjectGet(obj.Id)
	if err != nil {
		return nil, err
	}
	// URL & method for create
	url := client.BuildUrl(fmt.Sprintf("/institutions/%s/objects.json", obj.InstitutionId))
	method := "POST"
	// URL & method for update
	if existingObj != nil {
		url = client.BuildUrl(fmt.Sprintf("/objects/%s", obj.Id))
		method = "PUT"
	}
	data, err := obj.SerializeForFluctus()
	request, err := client.NewJsonRequest(method, url.String(), bytes.NewBuffer(data))
	if err != nil {
		return nil, err
	}
	response, err := client.httpClient.Do(request)
	if err != nil {
		return nil, err
	}

	defer response.Body.Close()
	body, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}

	// Fluctus returns 201 (Created) on create, 204 (No content) on update
	if response.StatusCode != 201 && response.StatusCode != 204 {
		err = fmt.Errorf("Expected status code 201 or 204 but got %d. URL: %s, Response Body: %s\n",
			response.StatusCode, request.URL, body)
		return nil, err
	}

	// On create, Fluctus returns the new object. On update, it returns nothing.
	if len(body) > 0 {
		//client.logger.Println(string(body))
		newObj = &models.IntellectualObject{}
		err = json.Unmarshal(body, newObj)
		if err != nil {
			return nil, err
		}
		return newObj, nil
	} else {
		return obj, nil
	}
}


/*
POST: http://localhost:3000/institutions/changeme:3/objects.json
{
"title": "Sample Postman Object",
"description": "This object was posted from Postman.",
"identifier": "fe986240-cf11-11e3-9c1a-0800200c9a66",
"rights": "consortial",
"authenticity_token": "i5tOg3jccd8XQm49SfdMiCb6S9QeZm3GrGVDsCJuGxs="
}
Response: {"title":["Sample Postman Object"],"description":["This object was posted from Postman."],"rights":["consortial"]}

Retrieve object by searching on identifier, like this (html, json):

http://localhost:3000/institutions/changeme:3/objects?utf8=%E2%9C%93&q=fe986240-cf11-11e3-9c1a-0800200c9a66
http://localhost:3000/institutions/changeme:3/objects.json?utf8=%E2%9C%93&q=fe986240-cf11-11e3-9c1a-0800200c9a66

Would be nice if Rails returned the internal id of the new object. That id is in the Location Header, e.g.:
http://localhost:3000/catalog/changeme:96

Response code should be 201.

Get events for this object:

GET http://localhost:3000/objects/changeme:95/events.json

Post a new event:

POST http://localhost:3000/objects/changeme:96/events.json

{
  "type": "fixodent-6765",
  "date_time": "",
  "detail": "Glued dentures to gums",
  "outcome": "success",
  "outcome_detail": "MD5:012345678ABCDEF01234",
  "outcome_information": "Multipart Put using md5 checksum",
  "object": "ruby aws-s3 gem",
  "agent": "https://github.com/marcel/aws-s3/tree/master",
  "authenticity_token": "i5tOg3jccd8XQm49SfdMiCb6S9QeZm3GrGVDsCJuGxs="
}

This endpoint is returning HTML. Should be returning JSON.

*/
