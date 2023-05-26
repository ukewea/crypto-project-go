package main

import (
	"fmt"
	"os"
	"strings"
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
	log.Level = logrus.TraceLevel

	conf, err := config.ReadConfig("config.toml")
	if err != nil {
		log.Fatal("Error reading config: ")
		log.Panic(err)
	}

	log.Debug("Config loaded successfully")

	log.Debug("Connecting to DB")
	dsn := fmt.Sprintf("host=%s user=%s password=%s dbname=%s port=%d sslmode=disable TimeZone=Asia/Taipei",
		conf.Database.Host, conf.Database.Username, conf.Database.Password, conf.Database.DBName, conf.Database.Port)

	// Mask password in logs
	log.Trace("DSN: ", strings.Replace(dsn, conf.Database.Password, "***(masked)***", 1))

	tradingSymbols := conf.Fetch.TradingSymbols
	vsCurrency := conf.Fetch.VSCurrency
	client := cryptocompare.NewClient(conf.Cryptocompare.APIKey, log)

	db, err := db.NewDB(dsn, log)
	if err != nil {
		log.Fatalf("Failed to connect to DB: %v", err)
		panic(err)
	}

	log.Debug("Successfully connected to DB")

	log.Infof("Starting data fetch for symbols: %v", tradingSymbols)

	for _, symbol := range tradingSymbols {
		log.Infof("Fetching hourly data for %s/%s", symbol, vsCurrency)

		// Fetch and save hourly data
		hourlyData, err := client.FetchAllHourlyOHLCVData(symbol, vsCurrency)
		if err != nil {
			log.Errorf("Failed to fetch hourly data for %s/%s, error: %v", symbol, vsCurrency, err)
			panic(err)
		}

		log.Infof("Successfully fetched hourly data for %s/%s, len: %d", symbol, vsCurrency, len(hourlyData))

		hourlyOHLCVData := make([]models.CryptoOHLCVHourly, len(hourlyData))
		for i, d := range hourlyData {
			hourlyOHLCVData[i] = models.CryptoOHLCVHourly{
				CryptoOHLCV: mapOHLCVData(&d, symbol, vsCurrency),
			}
		}

		if err := db.SaveHourlyOHLCData(hourlyOHLCVData); err != nil {
			log.Errorf("Failed to save hourly data for %s/%s, error: %v", symbol, vsCurrency, err)
			panic(err)
		}

		log.Infof("Successfully saved hourly data for %s/%s", symbol, vsCurrency)

		log.Infof("Fetching daily data for %s/%s", symbol, vsCurrency)

		// Fetch and save daily data
		dailyData, err := client.FetchAllDailyOHLCVData(symbol, vsCurrency)
		if err != nil {
			log.Errorf("Failed to fetch daily data for %s/%s, error: %v", symbol, vsCurrency, err)
			panic(err)
		}

		dailyOHLCVData := make([]models.CryptoOHLCVDaily, len(dailyData))
		for i, d := range dailyData {
			dailyOHLCVData[i] = models.CryptoOHLCVDaily{
				CryptoOHLCV: mapOHLCVData(&d, symbol, vsCurrency),
			}
		}

		log.Infof("Successfully fetched daily data for %s/%s, len: %d", symbol, vsCurrency, len(dailyData))

		if err := db.SaveDailyOHLCData(dailyOHLCVData); err != nil {
			log.Errorf("Failed to save daily data for %s/%s, error: %v", symbol, vsCurrency, err)
			panic(err)
		}

		log.Infof("Successfully saved daily data for %s/%s", symbol, vsCurrency)

		log.Infof("Fetching minute data for %s/%s", symbol, vsCurrency)

		// Fetch and save minute data
		minuteData, err := client.FetchAllMinuteOHLCVData(symbol, vsCurrency)
		if minuteData == nil && err != nil {
			log.Errorf("Failed to fetch minute data for %s/%s, error: %v", symbol, vsCurrency, err)
			panic(err)
		}

		minuteOHLCVData := make([]models.CryptoOHLCVMinute, len(minuteData))
		for i, d := range minuteData {
			minuteOHLCVData[i] = models.CryptoOHLCVMinute{
				CryptoOHLCV: mapOHLCVData(&d, symbol, vsCurrency),
			}
		}

		// If we failed to fetch all minute data, we will still save the data we have downloaded
		// Given that we can only download minute data for the past 7 days, we want to save as much data as possible
		if err != nil {
			log.Warnf("Failed to completely fetch minute data for %s/%s, but we will still save the data we have downloaded, error: %v",
				symbol, vsCurrency, err)
		} else {
			log.Infof("Successfully fetched minute data for %s/%s, len: %d", symbol, vsCurrency, len(minuteData))
		}

		if err := db.SaveMinuteOHLCData(minuteOHLCVData); err != nil {
			log.Errorf("Failed to save minute data for %s/%s, error: %v", symbol, vsCurrency, err)
			panic(err)
		}

		log.Infof("Successfully saved minute data for %s/%s", symbol, vsCurrency)
	}

	log.Infof("Data fetch completed for symbols: %v", tradingSymbols)
}

func mapOHLCVData(src *cryptocompare.OHLCVData, symbol string, vsCurrency string) models.CryptoOHLCV {
	return models.CryptoOHLCV{
		TradingSymbol: symbol,
		VsCurrency:    vsCurrency,
		Timestamp:     time.Unix(src.Time, 0).UTC(),
		Open:          src.Open,
		High:          src.High,
		Low:           src.Low,
		Close:         src.Close,
		VolumeFrom:    src.VolumeFrom,
		VolumeTo:      src.VolumeTo,
	}
}
