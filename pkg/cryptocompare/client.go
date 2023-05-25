package cryptocompare

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/shopspring/decimal"
	"github.com/sirupsen/logrus"
)

const (
	baseURL             = "https://min-api.cryptocompare.com/data/v2"
	histohourEndpoint   = "histohour"
	histodayEndpoint    = "histoday"
	histominuteEndpoint = "histominute"
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

func NewClient(apiKey string, logger *logrus.Logger) *Client {
	return &Client{
		apiKey:     apiKey,
		httpClient: &http.Client{},
		logger:     logrus.New(),
	}
}

func (c *Client) FetchMinuteOHLCData(tradingSymbol, vsCurrency string, limit int) ([]OHLCVData, error) {
	return c.fetchOHLCData(tradingSymbol, vsCurrency, limit, histominuteEndpoint)
}

func (c *Client) FetchHourlyOHLCData(tradingSymbol, vsCurrency string, limit int) ([]OHLCVData, error) {
	return c.fetchOHLCData(tradingSymbol, vsCurrency, limit, histohourEndpoint)
}

func (c *Client) FetchDailyOHLCData(tradingSymbol, vsCurrency string, limit int) ([]OHLCVData, error) {
	return c.fetchOHLCData(tradingSymbol, vsCurrency, limit, histodayEndpoint)
}

func (c *Client) fetchOHLCData(tradingSymbol, vsCurrency string, limit int, endpoint string) ([]OHLCVData, error) {
	url := fmt.Sprintf("%s/%s?fsym=%s&tsym=%s&limit=%d&api_key=%s", baseURL, endpoint, tradingSymbol, vsCurrency, limit, c.apiKey)

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
