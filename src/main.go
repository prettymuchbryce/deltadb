package main

import (
	"deltadb/src/db"
	"deltadb/src/net"
	"flag"
)

func main() {
	var configPath = flag.String("config", "", "Path to deltad.conf")
	flag.Parse()

	config := db.LoadConfig(configPath)
	context := db.NewContext(config)
	context.CreateDataDirIfDoesntExist()
	context.CreateAppendixIfDoesntExist()
	context.CreateBlobsDirIfDoesntExist()
	context.LoadAppendix()
	net.Start(context)
}
