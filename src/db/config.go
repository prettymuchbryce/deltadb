package db

import (
	"log"

	"code.google.com/p/gcfg"
)

type Config struct {
	Locations struct {
		Data string
	}
	Server struct {
		Port string
	}
}

func LoadConfig(path *string) *Config {
	var config Config
	err := gcfg.ReadFileInto(&config, *path)
	if err != nil {
		log.Fatal(err)
	}
	return &config
}
