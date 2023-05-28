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
	fetchAll := flag.Bool("fetch-all", false, "Fetch all data")
	flag.Parse()

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
	db, err := connectToDB(conf, log)
	if err != nil {
		log.Fatalf("Failed to connect to DB: %v", err)
		panic(err)
	}

	log.Debug("Successfully connected to DB")

	timeframes, limits := getTimeframesAndLimits(fetchAll, conf, log)
	tradingSymbols := conf.Fetch.TradingSymbols
	vsCurrency := conf.Fetch.VSCurrency

	downloadChannel := make(chan downloadJob, 10)
	saveChannel := make(chan saveJob, 10)
	var wg sync.WaitGroup

	defer close(downloadChannel)
	defer close(saveChannel)

	go downloadWorker(downloadChannel, saveChannel, conf.Cryptocompare.APIKey, log)
	go saveWorker(saveChannel, db, log)

	for _, symbol := range tradingSymbols {
		for i, timeframe := range timeframes {
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

// connectToDB connects to the database and returns a db.DB object on success
func connectToDB(conf *config.Config, log *logrus.Logger) (*db.DB, error) {
	dsn := fmt.Sprintf("host=%s user=%s password=%s dbname=%s port=%d sslmode=disable TimeZone=Asia/Taipei",
		conf.Database.Host, conf.Database.Username, conf.Database.Password, conf.Database.DBName, conf.Database.Port)

	// Mask password in logs
	log.Trace("DSN: ", strings.Replace(dsn, conf.Database.Password, "***(masked)***", 1))

	return db.NewDB(dsn, log)
}

// getTimeframesAndLimits returns timeframes and limits when downloading data
func getTimeframesAndLimits(fetchAll *bool, conf *config.Config, log *logrus.Logger) ([]string, []int) {
	// timeframes 有三個值，分別是 hourly, daily, minute，用來決定要下載哪個時間區間的資料
	// 1. 理論上只要 minutes 就可以推算出 hourly 和 daily 的資料
	//    但是 cryptocompare 的 API 限制 minute 資料只能取最近 7 天
	// 2. daily 理論上可以由 hourly 推算出來，但是有現成 API 可以用，就不自己算了
	timeframes := []string{"hourly", "daily", "minute"}
	var limits []int
	tradingSymbols := conf.Fetch.TradingSymbols

	if *fetchAll {
		log.Warnf("Fetching all data for symbols: %v", tradingSymbols)
		// set limits to -1 to fetch all data
		limits = []int{-1, -1, -1}
	} else {
		log.Infof("Fetching recent data for symbols: %v", tradingSymbols)
		log.Infof("Limits: hourly=%d, daily=%d, minute=%d", conf.Fetch.LimitHourly, conf.Fetch.LimitDaily, conf.Fetch.LimitMinute)
		limits = []int{conf.Fetch.LimitHourly, conf.Fetch.LimitDaily, conf.Fetch.LimitMinute}
	}

	return timeframes, limits
}

// downloadWorker downloads data from cryptocompare and sends it to saveChannel
func downloadWorker(downloadChannel chan downloadJob, saveChannel chan saveJob, apiKey string, log *logrus.Logger) {
	for job := range downloadChannel {
		func() {
			defer job.wg.Done()

			log.Infof("Fetching %s data of %s/%s", job.timeframe, job.symbol, job.vsCurrency)
			var data []cryptocompare.OHLCVData
			var err error

			client := cryptocompare.NewClient(apiKey, log)
			fetchAll := job.limit < 0

			funcsFetchAll := map[string]func(string, string) ([]cryptocompare.OHLCVData, error){
				"hourly": client.FetchAllHourlyOHLCVData,
				"daily":  client.FetchAllDailyOHLCVData,
				"minute": client.FetchAllMinuteOHLCVData,
			}

			funcsFetchLimit := map[string]func(string, string, int) ([]cryptocompare.OHLCVData, error){
				"hourly": client.FetchHourlyOHLCVData,
				"daily":  client.FetchDailyOHLCVData,
				"minute": client.FetchMinuteOHLCVData,
			}

			if fetchAll {
				if fetchAllFunc, ok := funcsFetchAll[job.timeframe]; !ok {
					log.Errorf("Invalid timeframe of fetch all job: %s", job.timeframe)
					return
				} else {
					data, err = fetchAllFunc(job.symbol, job.vsCurrency)
				}
			} else {
				if fetchLimitFunc, ok := funcsFetchLimit[job.timeframe]; !ok {
					log.Errorf("Invalid timeframe of fetch limit job: %s", job.timeframe)
					return
				} else {
					data, err = fetchLimitFunc(job.symbol, job.vsCurrency, job.limit)
				}
			}

			if data == nil && err != nil {
				log.Errorf("Failed to fetch %s data of %s/%s, error: %v", job.timeframe, job.symbol, job.vsCurrency, err)
				return
			} else if data == nil {
				log.Errorf("No error returned but data is nil when fetching %s data of %s/%s", job.timeframe, job.symbol, job.vsCurrency)
			} else if err != nil {
				log.Warnf("Failed to completely fetch %s data of %s/%s, but we will still save the data we have downloaded, error: %v",
					job.timeframe, job.symbol, job.vsCurrency, err)
			} else {
				log.Infof("Successfully fetched %s data of %s/%s, len: %d", job.timeframe, job.symbol, job.vsCurrency, len(data))
			}

			if fetchAll {
				data = removeInvalidOHLCVData(data)
			}

			log.Tracef("Sending %s data of %s/%s to saveChannel", job.timeframe, job.symbol, job.vsCurrency)
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

// saveWorker gets data from downloadWorker and saves it to DB
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

// mapOHLCVData maps cryptocompare.OHLCVData to models.CryptoOHLCV
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

// removeInvalidOHLCVData removes OHLCV data with all zero price values
func removeInvalidOHLCVData(data []cryptocompare.OHLCVData) []cryptocompare.OHLCVData {
	zero := decimal.NewFromInt(0)

	for i := len(data) - 1; i >= 0; i-- {
		if data[i].Open.Equal(zero) && data[i].High.Equal(zero) && data[i].Low.Equal(zero) && data[i].Close.Equal(zero) {
			data = append(data[:i], data[i+1:]...)
		}
	}
	return data
}
