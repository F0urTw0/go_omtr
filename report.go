package omniture

import (
	"encoding/json"
	"fmt"
	"os"
	"time"
)

func formatErrorResponse(resp []byte) error {
	var ge getError
	err := json.Unmarshal(resp, &ge)
	if err != nil {
		return fmt.Errorf("Report.Get returned '%s'; error attempting to unmarshal to error structure: %v", string(resp), err)
	}
	return ge
}

//  Queue a report to run
func (omcl *OmnitureClient) QueueReport(query *ReportQuery) (int64, error) {

	// debug mode
	if os.Getenv("debug") != "" {
		fmt.Printf("query: %v\n", query)
	}

	status, b, err := omcl.request("Report.Queue", query)

	if err != nil {
		return -1, err
	}

	if status == 400 {
		return -1, formatErrorResponse(b)
	}

	response := queueReportResponse{}

	if err = json.Unmarshal(b, &response); err != nil {
		return -1, err
	}

	return int64(response.ReportID), nil
}

// takes a reportId and returns a raw byteslice of json data, or error, including the Report Not Ready error.
func (omcl *OmnitureClient) GetReportRaw(reportId int64) ([]byte, error) {

	status, response, err := omcl.request("Report.Get", map[string]interface{}{
		"reportID": reportId,
	})

	if err != nil {
		return nil, err
	}

	// the api returns 400 if the report is not yet ready; in this case I'll parse the response as an error and return it
	if status == 400 {
		return nil, formatErrorResponse(response)
	}

	return response, err
}

//  Retrieves a list of possible valid elements for a report
func (omcl *OmnitureClient) GetElements(params map[string]interface{}) (elements []ReportElement, err error) {

	var (
		status int
		response []byte
	)

	//params := fmt.Sprintf("{ \"reportID\":%d }", reportId)

	if status, response, err = omcl.request("Report.GetElements", params); err != nil {
		return
	}

	// the api returns 400 if the report is not yet ready; in this case I'll parse the response as an error and return it
	if status == 400 {
		return nil, formatErrorResponse(response)
	}

	// parse return
	if err = json.Unmarshal(response,&elements); err != nil {
		return
	}

	return

}

func (omcl *OmnitureClient) GetReport(reportId int64) (*ReportResponse, error) {
	bytes, err := omcl.GetReportRaw(reportId)
	if err != nil {
		return nil, err
	}

	resp := &ReportResponse{}

	// debug mode
	if os.Getenv("debug") != "" {
		fmt.Printf("data: %s\n", string(bytes))
	}

	err = json.Unmarshal(bytes, resp)

	resp.TimeRetrieved = time.Now()

	return resp, err
}

/*
	Takes a report definition and a callback which will be called once the report has successfully been retrieved.
	Returns the reportId of the queued report or error
*/
func (omcl *OmnitureClient) Report(query *ReportQuery, successCallback func(*ReportResponse, error)) (int64, error) {
	rid, err := omcl.QueueReport(query)
	if err != nil {
		return -1, err
	}

	go omcl.waitForReportThenCall(rid, successCallback)

	return rid, nil
}

func (omcl *OmnitureClient) waitForReportThenCall(rid int64, callback func(*ReportResponse, error)) {
	for {
		response, err := omcl.GetReport(rid)

		if err == nil {
			callback(response, nil)
			return
		}

		if _, ok := err.(getError); !ok { // type assert that err is a getError, and execute following block if it's not
			// getError is a "good" error; anything else isn't.
			callback(nil, err)
			return
		}

		time.Sleep(1 * time.Second)
	}
}
