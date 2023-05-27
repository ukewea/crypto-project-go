package models

import (
	"time"

	"github.com/shopspring/decimal"
)

type CryptoOHLCV struct {
	ID            uint            `gorm:"primaryKey"`
	TradingSymbol string          `gorm:"type:varchar(10);index:,unique,composite:tpair_ts;index:,composite:tpair;not null"`
	VsCurrency    string          `gorm:"type:varchar(10);index:,unique,composite:tpair_ts;index:,composite:tpair;not null"`
	Timestamp     time.Time       `gorm:"type:timestamptz;index:,unique,composite:tpair_ts;not null"`
	Open          decimal.Decimal `gorm:"type:numeric;not null"`
	High          decimal.Decimal `gorm:"type:numeric;not null"`
	Low           decimal.Decimal `gorm:"type:numeric;not null"`
	Close         decimal.Decimal `gorm:"type:numeric;not null"`
	VolumeFrom    decimal.Decimal `gorm:"type:numeric;not null"`
	VolumeTo      decimal.Decimal `gorm:"type:numeric;not null"`
}

type CryptoOHLCVMinute struct {
	CryptoOHLCV
}

type CryptoOHLCVHourly struct {
	CryptoOHLCV
}

type CryptoOHLCVDaily struct {
	CryptoOHLCV
}

func (CryptoOHLCVMinute) TableName() string {
	return "crypto_ohlcv_minute_go"
}

func (CryptoOHLCVHourly) TableName() string {
	return "crypto_ohlcv_hourly_go"
}

func (CryptoOHLCVDaily) TableName() string {
	return "crypto_ohlcv_daily_go"
}
