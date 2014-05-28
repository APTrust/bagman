// Package client provides a client for the fluctus REST API.
package client

import (
	"io/ioutil"
	"encoding/json"
	"net/http"
	"net/url"
	"net/http/cookiejar"
	"regexp"
	"fmt"
	"bytes"
	"github.com/APTrust/bagman"
	"github.com/APTrust/bagman/fluctus/models"
)

type Client struct {
	hostUrl        string
	apiUser        *models.User
	csrfToken      string
	httpClient     *http.Client
}

// Creates a new fluctus client. Param hostUrl should come from
// the config.json file. Param apiEmail is the email address of
// a valid fluctus user. Param apiPassword is the user's password
// for fluctus. This will change in future, when the API key
// auth method is fixed in fluctus. At that point, we'll use the
// API key instead of the password.
func New(hostUrl string, apiEmail string, apiPassword string) (*Client, error) {
	// see security warning on nil PublicSuffixList here:
	// http://gotour.golang.org/src/pkg/net/http/cookiejar/jar.go?s=1011:1492#L24
	cookieJar, err := cookiejar.New(nil)
	if err != nil {
		return nil, fmt.Errorf("Can't create cookie jar for HTTP client: %v", err)
	}
	httpClient := &http.Client{ Jar: cookieJar }
	apiUser := &models.User{apiEmail, apiPassword, ""}
	return &Client{hostUrl, apiUser, "", httpClient}, nil
}

// Initializes a new API session. As of 4/28/2014, the Fluctus API
// is using session authentication because of some bugs in its
// implementation of API secret key authentication.
func (client *Client)InitSession() (error) {
	// Get the CSRF token... we can't submit the login form without this.
	loginUrl := client.BuildUrl("/users/sign_in?json")
	err := client.RequestCsrfToken(loginUrl.String())
	if err != nil {
		return fmt.Errorf("Error fetching CSRF token: %v", err)
	}
	// Now submit the login form with our token.
	req, err := http.NewRequest("POST", loginUrl.String(), nil)
	req.Header.Add("Content-Type", "application/json")
	jsonData, err := json.Marshal(client.apiUser)
	if err != nil {
		return fmt.Errorf("Error marshalling models.User to JSON: %v", err)
	}
	resp, err := client.httpClient.Post(loginUrl.String(), "application/json", bytes.NewReader(jsonData))
	if err != nil {
		return fmt.Errorf("Post to fluctus login controller returned error: %v", err)
	}
	if resp.StatusCode != 200 {
		fmt.Errorf("Fluctus authentication error. Got HTTP status %d: %v", resp.StatusCode, err)
	}


	for _, c := range client.httpClient.Jar.Cookies(loginUrl) {
		fmt.Println(c)
	}
	return nil
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


// Get the CSRF token from the login page. Simply requesting the
// login form will return an HTML page containing the CSRF token.
// (Even though we are requesting JSON, Rails returns an HTML
// form.)
func (client *Client) RequestCsrfToken(url string) (error) {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return fmt.Errorf("Error building GET request to fluctus: %v", err)
	}
	req.Header.Add("Content-Type", "application/json")
	response, err := client.httpClient.Do(req)
	if err != nil {
		return err
	}
	body, err := ioutil.ReadAll(response.Body)
	defer response.Body.Close()
	if err != nil {
		return fmt.Errorf("Error reading body of fluctus HTTP response: %v", err)
	}
	csrfToken, err := ExtractCsrfToken(string(body))
	if err != nil {
		return err
	}
	fmt.Println("CSRF Token =", csrfToken)
	client.csrfToken = csrfToken
	return nil
}

// This is a hack, but at the moment, fluctus is returning HTML for
// a JSON request. This uses a regex to extract the CSRF token.
// The Rails app should not be returning HTML for a JSON request,
// and should not require the CSRF token for JSON API requests.
// Until those issues are fixed, this function is the band-aid.
func ExtractCsrfToken(html string) (string, error) {
	// <meta content="vI9rpwFSa1xn4DQjkPAW/AxZxt8QAw7Cf28x4YrMfQY=" name="csrf-token" />
	re, err := regexp.Compile(`<meta\s+content="([^"]+)"\s+name="csrf-token"\s+/>`)
	if err != nil {
		return "", fmt.Errorf("Error compling regex for CSRF token: %v", err)
	}
	matches := re.FindAllStringSubmatch(html, 1)
	if len(matches) > 0 {
		return matches[0][1], err
	}
	return "", err
}

// GetBagStatus returns the status of a bag from a prior round of processing.
// We can get 0..n records for a bag: Zero records if we've never
// tried to process it before; one record if we processed a single
// uploaded version of a bag; multiple records if we've processed
// multiple identical uploaded versions of the bag.
func (client *Client) GetBagStatus(name, etag string) (status []*bagman.ProcessStatus, err error) {
	// url := client.BuildUrl("/itemstatus")
	// TODO: GET with name and etag, parse & return result
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
