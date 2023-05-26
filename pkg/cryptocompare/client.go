package cryptocompare

import (
	"encoding/json"
	"fmt"
	"net/http"
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
	Response string `json:"Response"`
	Message  string `json:"Message"`
	Data     struct {
		Data []OHLCVData `json:"Data"`
	} `json:"Data"`
}

// NewClient creates a new Client with given API key and logger
func NewClient(apiKey string, logger *logrus.Logger) *Client {
	return &Client{
		apiKey:     apiKey,
		httpClient: &http.Client{},
		logger:     logrus.New(),
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

// FetchAllAllMinuteOHLCVData fetches all minute-level OHLCV data
func (c *Client) FetchAllAllMinuteOHLCVData(tradingSymbol, vsCurrency string, limit int) ([]OHLCVData, error) {
	c.logger.Trace("Fetching all minute-level OHLCV data")
	return c.fetchAllOHLCVData(tradingSymbol, vsCurrency, histominuteEndpoint)
}

// FetchAllHourlyOHLCVData fetches all available hourly-level OH// FetchAllHourlyOHLCVData fetches all available hourly-level OHLCV data from the CryptoCompare API.
func (c *Client) FetchAllHourlyOHLCVData(tradingSymbol, vsCurrency string, limit int) ([]OHLCVData, error) {
	c.logger.Trace("Initiating FetchAllHourlyOHLCVData request.")
	return c.fetchAllOHLCVData(tradingSymbol, vsCurrency, histohourEndpoint)
}

// FetchAllDailyOHLCVData fetches all available daily-level OHLCV data from the CryptoCompare API.
func (c *Client) FetchAllDailyOHLCVData(tradingSymbol, vsCurrency string, limit int) ([]OHLCVData, error) {
	c.logger.Trace("Initiating FetchAllDailyOHLCVData request.")
	return c.fetchAllOHLCVData(tradingSymbol, vsCurrency, histodayEndpoint)
}

// fetchAllOHLCVData fetches all available OHLCV data of a specific frequency from the CryptoCompare API.
func (c *Client) fetchAllOHLCVData(tradingSymbol, vsCurrency string, endpoint string) ([]OHLCVData, error) {
	var allData []OHLCVData
	var toTs int64 = time.Now().Unix()

	c.logger.Info("Starting fetchAllOHLCVData request.")

	for {
		c.logger.Debug("Fetching more data in fetchAllOHLCVData.")
		data, err := c.fetchOHLCVDataWithTs(tradingSymbol, vsCurrency, apiMaxLimit, endpoint, toTs)
		if err != nil {
			c.logger.Errorf("Error in fetchOHLCVDataWithTs: %v", err)
			return nil, err
		}
		if len(data) == 0 {
			break
		}

		allData = append(allData, data...)
		toTs = data[len(data)-1].Time
	}

	c.logger.Info("Completed fetchAllOHLCVData request.")
	return allData, nil
}

func (c *Client) fetchOHLCVData(tradingSymbol, vsCurrency string, limit int, endpoint string) ([]OHLCVData, error) {
	url := fmt.Sprintf("%s/%s?fsym=%s&tsym=%s&limit=%d&api_key=%s", baseURL, endpoint, tradingSymbol, vsCurrency, limit, c.apiKey)
	return c.getOHLCVDataFromApi(url)
}

func (c *Client) fetchOHLCVDataWithTs(tradingSymbol, vsCurrency string, limit int, endpoint string, toTs int64) ([]OHLCVData, error) {
	url := fmt.Sprintf("%s/%s?fsym=%s&tsym=%s&limit=%d&toTs=%d&api_key=%s", baseURL, endpoint, tradingSymbol, vsCurrency, limit, toTs, c.apiKey)
	return c.getOHLCVDataFromApi(url)
}

func (c *Client) getOHLCVDataFromApi(url string) ([]OHLCVData, error) {
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

	return cr.Data.Data, nil
}
