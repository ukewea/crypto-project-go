package main

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"crypto_project/config"
	"crypto_project/pkg/cryptocompare"
	"crypto_project/pkg/db"
	"crypto_project/pkg/models"

	"github.com/shopspring/decimal"
	"github.com/sirupsen/logrus"
)

type downloadJob struct {
	symbol     string
	vsCurrency string
	timeframe  string
	limit      int
	wg         *sync.WaitGroup
}

type saveJob struct {
	symbol     string
	vsCurrency string
	data       []cryptocompare.OHLCVData
	timeframe  string
	wg         *sync.WaitGroup
}

func main() {
	log := logrus.New()
	log.Out = os.Stdout
	log.Level = logrus.DebugLevel

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

	fetchAll := flag.Bool("fetch-all", false, "Fetch all data")
	flag.Parse()
	var limits []int

	if *fetchAll {
		log.Warnf("Fetching all data for symbols: %v", tradingSymbols)
		// set limits to -1 to fetch all data
		limits = []int{-1, -1, -1}
	} else {
		log.Infof("Fetching recent data for symbols: %v", tradingSymbols)
		log.Infof("Limits: hourly=%d, daily=%d, minute=%d", conf.Fetch.LimitHourly, conf.Fetch.LimitDaily, conf.Fetch.LimitMinute)
		limits = []int{conf.Fetch.LimitHourly, conf.Fetch.LimitDaily, conf.Fetch.LimitMinute}
	}

	downloadChannel := make(chan downloadJob, 10)
	saveChannel := make(chan saveJob, 10)
	var wg sync.WaitGroup

	go downloadWorker(downloadChannel, saveChannel, client, log)
	go saveWorker(saveChannel, db, log)

	for _, symbol := range tradingSymbols {
		for i, timeframe := range []string{"hourly", "daily", "minute"} {
			wg.Add(1)
			downloadChannel <- downloadJob{
				symbol:     symbol,
				vsCurrency: vsCurrency,
				timeframe:  timeframe,
				limit:      limits[i],
				wg:         &wg,
			}
		}
	}

	wg.Wait()

	log.Infof("Data fetch completed for symbols: %v", tradingSymbols)
}

func downloadWorker(downloadChannel chan downloadJob, saveChannel chan saveJob, client *cryptocompare.Client, log *logrus.Logger) {
	for job := range downloadChannel {
		func() {
			defer job.wg.Done()

			log.Infof("Fetching %s data of %s/%s", job.timeframe, job.symbol, job.vsCurrency)
			var data []cryptocompare.OHLCVData
			var err error
			fetchAll := job.limit < 0

			switch job.timeframe {
			case "hourly":
				if fetchAll {
					data, err = client.FetchAllHourlyOHLCVData(job.symbol, job.vsCurrency)
				} else {
					data, err = client.FetchHourlyOHLCVData(job.symbol, job.vsCurrency, job.limit)
				}
			case "daily":
				if fetchAll {
					data, err = client.FetchAllDailyOHLCVData(job.symbol, job.vsCurrency)
				} else {
					data, err = client.FetchDailyOHLCVData(job.symbol, job.vsCurrency, job.limit)
				}
			case "minute":
				if fetchAll {
					data, err = client.FetchAllMinuteOHLCVData(job.symbol, job.vsCurrency)
				} else {
					data, err = client.FetchMinuteOHLCVData(job.symbol, job.vsCurrency, job.limit)
				}
			default:
				log.Errorf("Invalid timeframe: %s", job.timeframe)
				return
			}

			if data == nil && err != nil {
				log.Errorf("Failed to fetch %s data of %s/%s, error: %v", job.timeframe, job.symbol, job.vsCurrency, err)
				return
			} else if err != nil {
				log.Warnf("Failed to completely fetch %s data of %s/%s, but we will still save the data we have downloaded, error: %v",
					job.timeframe, job.symbol, job.vsCurrency, err)
			} else {
				log.Infof("Successfully fetched %s data of %s/%s, len: %d", job.timeframe, job.symbol, job.vsCurrency, len(data))
			}

			if fetchAll {
				data = removeInvalidOHLCVData(data)
			}

			job.wg.Add(1)
			saveChannel <- saveJob{
				symbol:     job.symbol,
				vsCurrency: job.vsCurrency,
				data:       data,
				timeframe:  job.timeframe,
				wg:         job.wg,
			}
		}()
	}
}

func saveWorker(saveChannel chan saveJob, db *db.DB, log *logrus.Logger) {
	for job := range saveChannel {
		func() {
			defer job.wg.Done()

			log.Infof("Saving %s data of %s/%s", job.timeframe, job.symbol, job.vsCurrency)
			var err error

			switch job.timeframe {
			case "hourly":
				hourlyOHLCVData := make([]models.CryptoOHLCVHourly, len(job.data))
				for i, d := range job.data {
					hourlyOHLCVData[i] = models.CryptoOHLCVHourly{
						CryptoOHLCV: mapOHLCVData(&d, job.symbol, job.vsCurrency),
					}
				}
				err = db.UpsertHourlyOHLCData(hourlyOHLCVData)
			case "daily":
				dailyOHLCVData := make([]models.CryptoOHLCVDaily, len(job.data))
				for i, d := range job.data {
					dailyOHLCVData[i] = models.CryptoOHLCVDaily{
						CryptoOHLCV: mapOHLCVData(&d, job.symbol, job.vsCurrency),
					}
				}
				err = db.UpsertDailyOHLCData(dailyOHLCVData)
			case "minute":
				minuteOHLCVData := make([]models.CryptoOHLCVMinute, len(job.data))
				for i, d := range job.data {
					minuteOHLCVData[i] = models.CryptoOHLCVMinute{
						CryptoOHLCV: mapOHLCVData(&d, job.symbol, job.vsCurrency),
					}
				}
				err = db.UpsertMinuteOHLCData(minuteOHLCVData)
			default:
				log.Errorf("Invalid timeframe: %s", job.timeframe)
				return
			}

			if err != nil {
				log.Errorf("Failed to save %s data of %s/%s, error: %v", job.timeframe, job.symbol, job.vsCurrency, err)
				return
			}

			log.Infof("Successfully saved %s data of %s/%s", job.timeframe, job.symbol, job.vsCurrency)
		}()
	}
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

func removeInvalidOHLCVData(data []cryptocompare.OHLCVData) []cryptocompare.OHLCVData {
	zero := decimal.NewFromInt(0)

	for i := len(data) - 1; i >= 0; i-- {
		if data[i].Open.Equal(zero) && data[i].High.Equal(zero) && data[i].Low.Equal(zero) && data[i].Close.Equal(zero) {
			data = append(data[:i], data[i+1:]...)
		}
	}
	return data
}
