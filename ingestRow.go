package p

import (
        "context"
        "fmt"
        "log"
        "time"
        "bufio"
        "encoding/json"
		"os"
        "cloud.google.com/go/storage"
        "cloud.google.com/go/bigquery"
)

type GCSEvent struct {
        Bucket string `json:"bucket"`
        Name   string `json:"name"`
}

type Item struct {
	Clientip                              string        `json:"ClientIP"`
	Clientrequesthost                     string        `json:"ClientRequestHost"`
	Clientrequestmethod                   string        `json:"ClientRequestMethod"`
	Clientrequesturi                      string        `json:"ClientRequestURI"`
	Edgeendtimestamp                      string        `json:"EdgeEndTimestamp"`
	Edgeresponsebytes                     int           `json:"EdgeResponseBytes"`
	Edgeresponsestatus                    int           `json:"EdgeResponseStatus"`
	Edgestarttimestamp                    string        `json:"EdgeStartTimestamp"`
	Rayid                                 string        `json:"RayID"`
	Cachecachestatus                      string        `json:"CacheCacheStatus"`
	Cachetieredfill                       bool          `json:"CacheTieredFill"`
	Cacheresponsebytes                    int           `json:"CacheResponseBytes"`
	Cacheresponsestatus                   int           `json:"CacheResponseStatus"`
	Firewallmatchesactions                []string      `json:"FirewallMatchesActions"`
	Firewallmatchesruleids                []string      `json:"FirewallMatchesRuleIDs"`
	Firewallmatchessources                []string      `json:"FirewallMatchesSources"`
	Origindnsresponsetimems               int           `json:"OriginDNSResponseTimeMs"`
	Originip                              string        `json:"OriginIP"`
	Originrequestheadersenddurationms     int           `json:"OriginRequestHeaderSendDurationMs"`
	Originsslprotocol                     string        `json:"OriginSSLProtocol"`
	Origintcphandshakedurationms          int           `json:"OriginTCPHandshakeDurationMs"`
	Origintlshandshakedurationms          int           `json:"OriginTLSHandshakeDurationMs"`
	Originresponsebytes                   int           `json:"OriginResponseBytes"`
	Originresponsedurationms              int           `json:"OriginResponseDurationMs"`
	Originresponsehttpexpires             string        `json:"OriginResponseHTTPExpires"`
	Originresponsehttplastmodified        string        `json:"OriginResponseHTTPLastModified"`
	Originresponseheaderreceivedurationms int           `json:"OriginResponseHeaderReceiveDurationMs"`
	Originresponsestatus                  int           `json:"OriginResponseStatus"`
	Originresponsetime                    int           `json:"OriginResponseTime"`
	Wafaction                             string        `json:"WAFAction"`
	Wafflags                              string        `json:"WAFFlags"`
	Wafmatchedvar                         string        `json:"WAFMatchedVar"`
	Wafprofile                            string        `json:"WAFProfile"`
	Wafruleid                             string        `json:"WAFRuleID"`
	Wafrulemessage                        string        `json:"WAFRuleMessage"`
	Workercputime                         int           `json:"WorkerCPUTime"`
	Workerstatus                          string        `json:"WorkerStatus"`
	Workersubrequest                      bool          `json:"WorkerSubrequest"`
	Workersubrequestcount                 int           `json:"WorkerSubrequestCount"`
	Parentrayid                           string        `json:"ParentRayID"`
	Uppertiercoloid                       int           `json:"UpperTierColoID"`
	Zoneid                                int           `json:"ZoneID"`
	Zonename                              string        `json:"ZoneName"`
	Smartroutecoloid                      int           `json:"SmartRouteColoID"`
	Securitylevel                         string        `json:"SecurityLevel"`
	Clientasn                             int           `json:"ClientASN"`
	Clientcountry                         string        `json:"ClientCountry"`
	Clientdevicetype                      string        `json:"ClientDeviceType"`
	Clientipclass                         string        `json:"ClientIPClass"`
	Clientmtlsauthcertfingerprint         string        `json:"ClientMTLSAuthCertFingerprint"`
	Clientmtlsauthstatus                  string        `json:"ClientMTLSAuthStatus"`
	Clientrequestbytes                    int           `json:"ClientRequestBytes"`
	Clientrequestpath                     string        `json:"ClientRequestPath"`
	Clientrequestprotocol                 string        `json:"ClientRequestProtocol"`
	Clientrequestreferer                  string        `json:"ClientRequestReferer"`
	Clientrequestsource                   string        `json:"ClientRequestSource"`
	Clientrequestuseragent                string        `json:"ClientRequestUserAgent"`
	Clientsslcipher                       string        `json:"ClientSSLCipher"`
	Clientsslprotocol                     string        `json:"ClientSSLProtocol"`
	Clientsrcport                         int           `json:"ClientSrcPort"`
	Clienttcprttms                        int           `json:"ClientTCPRTTMs"`
	Clientxrequestedwith                  string        `json:"ClientXRequestedWith"`
	Edgecfconnectingo2O                   bool          `json:"EdgeCFConnectingO2O"`
	Edgecolocode                          string        `json:"EdgeColoCode"`
	Edgecoloid                            int           `json:"EdgeColoID"`
	Edgepathingop                         string        `json:"EdgePathingOp"`
	Edgepathingsrc                        string        `json:"EdgePathingSrc"`
	Edgepathingstatus                     string        `json:"EdgePathingStatus"`
	Edgeratelimitaction                   string        `json:"EdgeRateLimitAction"`
	Edgeratelimitid                       int           `json:"EdgeRateLimitID"`
	Edgerequesthost                       string        `json:"EdgeRequestHost"`
	Edgeresponsebodybytes                 int           `json:"EdgeResponseBodyBytes"`
	Edgeresponsecompressionratio          float32       `json:"EdgeResponseCompressionRatio"`
	Edgeresponsecontenttype               string        `json:"EdgeResponseContentType"`
	Edgeserverip                          string        `json:"EdgeServerIP"`
	Edgetimetofirstbytems                 int           `json:"EdgeTimeToFirstByteMs"`
}

func IngestRow(ctx context.Context, e GCSEvent) error{
		    datasetID := os.Getenv("DATASET")
		    tableID := os.Getenv("TABLE")
		    projectID := os.Getenv("PROJECT")
        client, err := storage.NewClient(ctx)
        if err != nil {
                log.Fatal(err)
        }
                bqClient, err := bigquery.NewClient(ctx, projectID)
                if err != nil {
                                return fmt.Errorf("bigquery.NewClient: %v", err)
                }
                defer bqClient.Close()

		
        read(bqClient, client, datasetID, tableID, e.Bucket, e.Name)
        return nil
}
func read(bqClient *bigquery.Client, client *storage.Client, datasetID, tableID, bucket, object string) {
        // [START download_file]
        ctx := context.Background()
        ctx, cancel := context.WithTimeout(ctx, time.Second*50)
        defer cancel()
        rc, err := client.Bucket(bucket).Object(object).NewReader(ctx)
        if err != nil {
                log.Fatal(err)
        }
        defer rc.Close()

        scanner := bufio.NewScanner(rc)
        for scanner.Scan() {
                        if err := insertRows(bqClient, ctx, scanner.Text(), datasetID, tableID); err != nil{
                            log.Fatal(err)
                        }
        }

        if err := scanner.Err(); err != nil {
          log.Fatal(err)
        }
        // [END download_file]
}

func insertRows(bqClient *bigquery.Client, ctx context.Context, row, datasetID, tableID string) error {
       jsonData := []byte(row)
       var structRow Item
        err := json.Unmarshal(jsonData, &structRow)
        if err != nil {
            log.Fatal(err) 
        }
        items := []Item{
                structRow,
        }

        inserter := bqClient.Dataset(datasetID).Table(tableID).Inserter()
        if err := inserter.Put(ctx, items); err != nil {
                        return err
        }
        return nil
}
