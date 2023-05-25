package main

import (
	"fmt"
	"os"
	"time"

	"crypto_project/config"
	"crypto_project/pkg/cryptocompare"
	"crypto_project/pkg/db"
	"crypto_project/pkg/models"

	"github.com/sirupsen/logrus"
)

func main() {
	log := logrus.New()
	log.Out = os.Stdout
	log.Level = logrus.DebugLevel

	conf, err := config.ReadConfig("config.toml")
	if err != nil {
		log.Fatal("Error reading config: ")
		log.Panic(err)
	}

	dsn := fmt.Sprintf("host=%s user=%s password=%s dbname=%s port=%d sslmode=disable TimeZone=Asia/Taipei",
		conf.Database.Host, conf.Database.Username, conf.Database.Password, conf.Database.DBName, conf.Database.Port)

	tradingSymbols := conf.Fetch.TradingSymbols
	vsCurrency := conf.Fetch.VSCurrency
	limitDaily := conf.Fetch.LimitDaily
	limitHourly := conf.Fetch.LimitHourly

	log.Infof("Starting data fetch for symbols: %v", tradingSymbols)

	client := cryptocompare.NewClient(conf.Cryptocompare.APIKey, log)

	db, err := db.NewDB(dsn, log)
	if err != nil {
		log.Fatalf("Failed to connect to DB: %v", err)
		panic(err)
	}

	for _, symbol := range tradingSymbols {
		log.Debugf("Fetching hourly data for symbol: %s", symbol)

		// Fetch and save hourly data
		hourlyData, err := client.FetchHourlyOHLCData(symbol, vsCurrency, limitHourly)
		if err != nil {
			log.Errorf("Failed to fetch hourly data for symbol: %s, error: %v", symbol, err)
			panic(err)
		}

		log.Debugf("Successfully fetched hourly data for symbol: %s", symbol)

		hourlyOHLCVData := make([]models.CryptoOHLCVHourly, len(hourlyData))
		for i, d := range hourlyData {
			hourlyOHLCVData[i] = models.CryptoOHLCVHourly{
				CryptoOHLCV: models.CryptoOHLCV{
					TradingSymbol: symbol,
					VsCurrency:    vsCurrency,
					Timestamp:     time.Unix(d.Time, 0).UTC(),
					Open:          d.Open,
					High:          d.High,
					Low:           d.Low,
					Close:         d.Close,
					VolumeFrom:    d.VolumeFrom,
					VolumeTo:      d.VolumeTo,
				},
			}
		}

		if err := db.SaveHourlyOHLCData(hourlyOHLCVData); err != nil {
			log.Errorf("Failed to save hourly data for symbol: %s, error: %v", symbol, err)
			panic(err)
		}

		log.Debugf("Successfully saved hourly data for symbol: %s", symbol)

		log.Debugf("Fetching daily data for symbol: %s", symbol)

		// Fetch and save daily data
		dailyData, err := client.FetchDailyOHLCData(symbol, vsCurrency, limitDaily)
		if err != nil {
			log.Errorf("Failed to fetch daily data for symbol: %s, error: %v", symbol, err)
			panic(err)
		}

		dailyOHLCVData := make([]models.CryptoOHLCVDaily, len(dailyData))
		for i, d := range dailyData {
			dailyOHLCVData[i] = models.CryptoOHLCVDaily{
				CryptoOHLCV: models.CryptoOHLCV{
					TradingSymbol: symbol,
					VsCurrency:    vsCurrency,
					Timestamp:     time.Unix(d.Time, 0).UTC(),
					Open:          d.Open,
					High:          d.High,
					Low:           d.Low,
					Close:         d.Close,
					VolumeFrom:    d.VolumeFrom,
					VolumeTo:      d.VolumeTo,
				},
			}
		}

		log.Debugf("Successfully fetched daily data for symbol: %s", symbol)

		if err := db.SaveDailyOHLCData(dailyOHLCVData); err != nil {
			log.Errorf("Failed to save daily data for symbol: %s, error: %v", symbol, err)
			panic(err)
		}

		log.Debugf("Successfully saved daily data for symbol: %s", symbol)
	}

	log.Infof("Data fetch completed for symbols: %v", tradingSymbols)
}
