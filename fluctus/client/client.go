// Package client provides a client for the fluctus REST API.
package client

import (
	"io/ioutil"
	"encoding/json"
	"net/http"
	"net/url"
	"net/http/cookiejar"
//	"regexp"
	"fmt"
//	"bytes"
	"log"
	"time"
	"github.com/APTrust/bagman"
//	"github.com/APTrust/bagman/fluctus/models"
)

type Client struct {
	hostUrl        string
	apiUser        string
	apiKey         string
	httpClient     *http.Client
	logger         *log.Logger
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
	httpClient := &http.Client{ Jar: cookieJar }
	return &Client{hostUrl, apiUser, apiKey, httpClient, logger}, nil
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
func (client *Client) NewJsonGet(url string) (*http.Request, error) {
	req, err := http.NewRequest("GET", url, nil)
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
// We can get 0..n records for a bag: Zero records if we've never
// tried to process it before; one record if we processed a single
// uploaded version of a bag; multiple records if we've processed
// multiple identical uploaded versions of the bag. Param bag_date
// comes from the LastModified property of the S3 key.
func (client *Client) GetBagStatus(etag, name string, bag_date time.Time) (status *bagman.ProcessStatus, err error) {
	// TODO: Add bag_date to url
	url := client.BuildUrl(fmt.Sprintf("/itemresults/%s/%s/", etag, name)) //, bag_date.String()))

	// Build the request
	req, err := client.NewJsonGet(url.String())
	if err != nil {
		return nil, err
	}

	// Make the request & make sure response is OK
	response, err := client.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	body, err := ioutil.ReadAll(response.Body)
	response.Body.Close()
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

// UpdateBagStatus sends a message to Fluctus describing whether bag
// processing succeeded or failed. If it failed, the ProcessStatus
// object includes some details of what went wrong.
func (client *Client) UpdateBagStatus(status *bagman.ProcessStatus) (err error) {
	// url := client.BuildUrl("/itemstatus")
	// TODO: POST data. Parse and return result.
	return nil
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
