package cryptocompare

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"time"

	"github.com/shopspring/decimal"
	"github.com/sirupsen/logrus"
)

const (
	baseURL             = "https://min-api.cryptocompare.com/data/v2"
	histohourEndpoint   = "histohour"
	histodayEndpoint    = "histoday"
	histominuteEndpoint = "histominute"
	apiMaxLimit         = 2000
)

type Client struct {
	apiKey     string
	httpClient *http.Client
	logger     *logrus.Logger
}

type OHLCVData struct {
	Time       int64           `json:"time"`
	Open       decimal.Decimal `json:"open"`
	High       decimal.Decimal `json:"high"`
	Low        decimal.Decimal `json:"low"`
	Close      decimal.Decimal `json:"close"`
	VolumeFrom decimal.Decimal `json:"volumefrom"`
	VolumeTo   decimal.Decimal `json:"volumeto"`
}

type CryptoResponse struct {
	Response   string `json:"Response"`
	Message    string `json:"Message"`
	HasWarning bool   `json:"HasWarning"`
	Type       int    `json:"Type"`
	Data       struct {
		Aggregated bool        `json:"Aggregated"`
		TimeFrom   int64       `json:"TimeFrom"`
		TimeTo     int64       `json:"TimeTo"`
		Data       []OHLCVData `json:"Data"`
	} `json:"Data"`
}

// NewClient creates a new Client with given API key and logger
func NewClient(apiKey string, logger *logrus.Logger) *Client {
	return &Client{
		apiKey:     apiKey,
		httpClient: &http.Client{},
		logger:     logger,
	}
}

// FetchMinuteOHLCVData fetches minute-level OHLCV data up to given limit
func (c *Client) FetchMinuteOHLCVData(tradingSymbol, vsCurrency string, limit int) ([]OHLCVData, error) {
	c.logger.Trace("Fetching minute-level OHLCV data")
	return c.fetchOHLCVData(tradingSymbol, vsCurrency, limit, histominuteEndpoint)
}

// FetchHourlyOHLCVData fetches hourly-level OHLCV data up to given limit
func (c *Client) FetchHourlyOHLCVData(tradingSymbol, vsCurrency string, limit int) ([]OHLCVData, error) {
	c.logger.Trace("Fetching hourly-level OHLCV data")
	return c.fetchOHLCVData(tradingSymbol, vsCurrency, limit, histohourEndpoint)
}

// FetchDailyOHLCVData fetches daily-level OHLCV data up to given limit
func (c *Client) FetchDailyOHLCVData(tradingSymbol, vsCurrency string, limit int) ([]OHLCVData, error) {
	c.logger.Trace("Fetching daily-level OHLCV data")
	return c.fetchOHLCVData(tradingSymbol, vsCurrency, limit, histodayEndpoint)
}

// FetchAllMinuteOHLCVData fetches all minute-level OHLCV data
func (c *Client) FetchAllMinuteOHLCVData(tradingSymbol, vsCurrency string) ([]OHLCVData, error) {
	c.logger.Trace("Fetching all minute-level OHLCV data")
	data, err := c.fetchAllOHLCVData(tradingSymbol, vsCurrency, histominuteEndpoint)
	if data == nil && err != nil {
		return nil, err
	}

	// remove last row if it's not ready yet
	data = removeNotReadyData(data)

	return data, err
}

// FetchAllHourlyOHLCVData fetches all hourly-level OHLCV data
func (c *Client) FetchAllHourlyOHLCVData(tradingSymbol, vsCurrency string) ([]OHLCVData, error) {
	c.logger.Trace("Initiating FetchAllHourlyOHLCVData request.")
	return c.fetchAllOHLCVData(tradingSymbol, vsCurrency, histohourEndpoint)
}

// FetchAllDailyOHLCVData fetches all available daily-level OHLCV data from the CryptoCompare API.
func (c *Client) FetchAllDailyOHLCVData(tradingSymbol, vsCurrency string) ([]OHLCVData, error) {
	c.logger.Trace("Initiating FetchAllDailyOHLCVData request.")
	return c.fetchAllOHLCVData(tradingSymbol, vsCurrency, histodayEndpoint)
}

// fetchAllOHLCVData fetches all available OHLCV data of a specific frequency from the CryptoCompare API.
func (c *Client) fetchAllOHLCVData(tradingSymbol, vsCurrency string, endpoint string) ([]OHLCVData, error) {
	c.logger.Info("Starting fetchAllOHLCVData request.")
	var allData []OHLCVData
	var err error = nil

	// Add 5 seconds to avoid losing data due to time difference
	var toTs int64 = time.Now().Unix() + 5

	for {
		c.logger.Debugf("Fetching more data for %s/%s in fetchAllOHLCVData, toTs: %s",
			tradingSymbol, vsCurrency, time.Unix(toTs, 0).In(time.UTC).Format(time.RFC3339))

		url := fmt.Sprintf("%s/%s?fsym=%s&tsym=%s&limit=%d&toTs=%d&api_key=%s",
			baseURL, endpoint, tradingSymbol, vsCurrency, apiMaxLimit, toTs, c.apiKey)
		c.logger.Trace("URL: ", url)

		var resp *CryptoResponse
		resp, err = c.getOHLCVResponseFromApi(url)
		if err != nil {
			c.logger.Errorf("Error in getOHLCVResponseFromApi for %s/%s: %v", tradingSymbol, vsCurrency, err)
			break
		}

		data := resp.Data.Data
		if len(data) == 0 {
			c.logger.Tracef("No more data to fetch OHLCV history for %s/%s in fetchAllOHLCVData", tradingSymbol, vsCurrency)
			break
		}

		if isVolumeFromZeroInDataSet(data) {
			if len(data) != 0 {
				c.logger.Warnf("Encountered fake dataset for %s/%s in fetchAllOHLCVData, stop the iteration", tradingSymbol, vsCurrency)
			}
			break
		}

		allData = append(allData, data...)
		toTs = resp.Data.TimeFrom - 1

		c.logger.Debugf("Pause before next fetchAllOHLCVData iteration for %s/%s...", tradingSymbol, vsCurrency)
		time.Sleep(10 * time.Second)
	}

	if len(allData) == 0 {
		return nil, err
	}

	sortByTime(allData)

	if err != nil {
		c.logger.Warnf("fetchAllOHLCVData request for %s/%s breaked early, return data it fetched so far, len: %d", tradingSymbol, vsCurrency, len(allData))
	} else {
		c.logger.Infof("Completed fetchAllOHLCVData request for %s/%s, len: %d", tradingSymbol, vsCurrency, len(allData))
	}

	return allData, err
}

func (c *Client) fetchOHLCVData(tradingSymbol, vsCurrency string, limit int, endpoint string) ([]OHLCVData, error) {
	url := fmt.Sprintf("%s/%s?fsym=%s&tsym=%s&limit=%d&api_key=%s",
		baseURL, endpoint, tradingSymbol, vsCurrency, limit, c.apiKey)

	if resp, err := c.getOHLCVResponseFromApi(url); err != nil {
		return nil, err
	} else {
		return resp.Data.Data, nil
	}
}

func (c *Client) getOHLCVResponseFromApi(url string) (*CryptoResponse, error) {
	c.logger.Debugf("Fetching data from URL: %s", url)

	resp, err := c.httpClient.Get(url)
	if err != nil {
		c.logger.Errorf("Error making HTTP GET request: %v", err)
		return nil, err
	}
	defer resp.Body.Close()

	var cr CryptoResponse
	if err := json.NewDecoder(resp.Body).Decode(&cr); err != nil {
		c.logger.Errorf("Error decoding HTTP response: %v", err)
		return nil, err
	}

	if cr.Response == "Error" {
		c.logger.Errorf("Error fetching data: %s", cr.Message)
		return nil, fmt.Errorf("error fetching data: %s", cr.Message)
	}

	c.logger.Debugf("Successfully fetched data from URL: %s", url)

	return &cr, nil
}

func isVolumeFromZeroInDataSet(data []OHLCVData) bool {
	for _, d := range data {
		if !d.VolumeFrom.IsZero() {
			return false
		}
	}
	return true
}

func removeNotReadyData(data []OHLCVData) []OHLCVData {
	if len(data) == 0 {
		return data
	}

	if data[len(data)-1].VolumeFrom.Equal(decimal.Zero) {
		return data[:len(data)-1]
	}

	return data
}

func sortByTime(data []OHLCVData) {
	sort.Slice(data, func(i, j int) bool {
		return data[i].Time < data[j].Time
	})
}
