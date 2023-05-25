package config

import "github.com/BurntSushi/toml"

type Config struct {
	Database struct {
		Host     string `toml:"host"`
		Port     int    `toml:"port"`
		Username string `toml:"username"`
		Password string `toml:"password"`
		DBName   string `toml:"dbname"`
	} `toml:"database"`
	Cryptocompare struct {
		APIKey string `toml:"api_key"`
	} `toml:"cryptocompare"`
	Fetch struct {
		TradingSymbols []string `toml:"trading_symbols"`
		VSCurrency     string   `toml:"vs_currency"`
		LimitDaily     int      `toml:"limit_daily"`
		LimitHourly    int      `toml:"limit_hourly"`
	} `toml:"fetch"`
}

func ReadConfig(filename string) (*Config, error) {
	var conf Config
	if _, err := toml.DecodeFile(filename, &conf); err != nil {
		return nil, err
	}
	return &conf, nil
}
