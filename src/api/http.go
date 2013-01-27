package api

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
)

func doRequest(method, url string, value interface{}) (int, error) {
	req, err := http.NewRequest(method, url, nil)
	if err != nil {
		return 0, fmt.Errorf("Request Error: %s %s %s", method, url, err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, fmt.Errorf("Respone Error: %s", method, url, err)
	}
	defer resp.Body.Close()
	return processResponse(resp, value)
}

func doPost(url string, form url.Values, value interface{}) (int, error) {
	resp, err := http.PostForm(url, form)
	if err != nil {
		return 0, fmt.Errorf("Post: %s", err)
	}
	defer resp.Body.Close()
	return processResponse(resp, value)
}

func processResponse(resp *http.Response, value interface{}) (int, error) {
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return 0, fmt.Errorf("Read Response: %s", err)
	}
	switch resp.StatusCode {
	case http.StatusInternalServerError, http.StatusNotFound:
		return resp.StatusCode, fmt.Errorf("Response Server Error: %s", body)
	}
	if err = json.Unmarshal(body, value); err != nil {
		return 0, fmt.Errorf("Json Decode: %s", err)
	}
	return resp.StatusCode, nil
}
