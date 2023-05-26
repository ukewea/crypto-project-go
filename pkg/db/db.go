package db

import (
	"github.com/sirupsen/logrus"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"crypto_project/pkg/models"
)

type DB struct {
	*gorm.DB
	Logger *logrus.Logger
}

func NewDB(dsn string, logger *logrus.Logger) (*DB, error) {
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		logger.Errorf("Error connecting to database: %v", err)
		return nil, err
	}

	db.AutoMigrate(&models.CryptoOHLCVMinute{}, &models.CryptoOHLCVHourly{}, &models.CryptoOHLCVDaily{})

	return &DB{db, logger}, nil
}

func (db *DB) UpsertMinuteOHLCData(data []models.CryptoOHLCVMinute) error {
	db.Logger.Trace("Starting saving minute data")
	clauses := db.Clauses(clause.OnConflict{
		Columns: []clause.Column{{Name: "trading_symbol"}, {Name: "vs_currency"}, {Name: "timestamp"}},
		DoUpdates: clause.AssignmentColumns([]string{
			"open", "high", "low", "close", "volume_from", "volume_to",
		}),
	})
	for _, d := range data {
		if err := clauses.Create(&d).Error; err != nil {
			db.Logger.Errorf("Error saving minute data: %v", err)
			return err
		}
	}
	db.Logger.Trace("Successfully saved minute data")
	return nil
}

func (db *DB) UpsertHourlyOHLCData(data []models.CryptoOHLCVHourly) error {
	db.Logger.Trace("Starting saving hourly data")
	clauses := db.Clauses(clause.OnConflict{
		Columns: []clause.Column{{Name: "trading_symbol"}, {Name: "vs_currency"}, {Name: "timestamp"}},
		DoUpdates: clause.AssignmentColumns([]string{
			"open", "high", "low", "close", "volume_from", "volume_to",
		}),
	})
	for _, d := range data {
		if err := clauses.Create(&d).Error; err != nil {
			db.Logger.Errorf("Error saving hourly data: %v", err)
			return err
		}
	}
	db.Logger.Trace("Successfully saved hourly data")
	return nil
}

func (db *DB) UpsertDailyOHLCData(data []models.CryptoOHLCVDaily) error {
	db.Logger.Trace("Starting saving daily data")
	clauses := db.Clauses(clause.OnConflict{
		Columns: []clause.Column{{Name: "trading_symbol"}, {Name: "vs_currency"}, {Name: "timestamp"}},
		DoUpdates: clause.AssignmentColumns([]string{
			"open", "high", "low", "close", "volume_from", "volume_to",
		}),
	})
	for _, d := range data {
		if err := clauses.Create(&d).Error; err != nil {
			db.Logger.Errorf("Error saving daily data: %v", err)
			return err
		}
	}
	db.Logger.Trace("Successfully saved daily data")
	return nil
}

func (db *DB) GetMinuteOHLCData(limit int, tradingSymbol string, vsCurrency string) ([]models.CryptoOHLCVMinute, error) {
	var data []models.CryptoOHLCVMinute
	result := db.Where("trading_symbol = ? AND vs_currency = ?", tradingSymbol, vsCurrency).
		Order("timestamp asc").
		Limit(limit).
		Find(&data)
	if result.Error != nil {
		db.Logger.Errorf("Error getting minute data: %v", result.Error)
		return nil, result.Error
	}
	return data, nil
}

func (db *DB) GetHourlyOHLCData(limit int, tradingSymbol string, vsCurrency string) ([]models.CryptoOHLCVHourly, error) {
	var data []models.CryptoOHLCVHourly
	result := db.Where("trading_symbol = ? AND vs_currency = ?", tradingSymbol, vsCurrency).
		Order("timestamp asc").
		Limit(limit).
		Find(&data)
	if result.Error != nil {
		db.Logger.Errorf("Error getting hourly data: %v", result.Error)
		return nil, result.Error
	}
	return data, nil
}

func (db *DB) GetDailyOHLCData(limit int, tradingSymbol string, vsCurrency string) ([]models.CryptoOHLCVDaily, error) {
	var data []models.CryptoOHLCVDaily
	result := db.Where("trading_symbol = ? AND vs_currency = ?", tradingSymbol, vsCurrency).
		Order("timestamp asc").
		Limit(limit).
		Find(&data)
	if result.Error != nil {
		db.Logger.Errorf("Error getting daily data: %v", result.Error)
		return nil, result.Error
	}
	return data, nil
}
