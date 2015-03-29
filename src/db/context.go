package db

import (
	"encoding/gob"
	"fmt"
	"log"
	"os"
	"strings"
)

type appendix struct {
	Collections map[string]collection
	CommitMap   map[uint64]bool
	Commits     []Commit
}

type Context struct {
	appendix  appendix
	Config    Config
	blobstore BlobStore
}

func (context *Context) GetCommitsTemp() []Commit {
	return context.appendix.Commits
}

func (context *Context) GetLatestHash() uint64 {
	if len(context.appendix.Commits) == 0 {
		return 0
	}
	return context.appendix.Commits[len(context.appendix.Commits)-1].Hash
}

func (context *Context) CreateDataDirIfDoesntExist() {
	data := context.Config.Locations.Data

	if _, err := os.Stat(data); err != nil {
		err := os.Mkdir(data, 0770)
		if err != nil {
			panic(err)
		}
	}
}

func (context *Context) CreateAppendixIfDoesntExist() {
	appendixFileName := getAppendixFilePath(context)

	if _, err := os.Stat(appendixFileName); err != nil {
		appendixFile, err := os.Create(appendixFileName)
		if err != nil {
			panic(err)
		}

		context.appendix = *new(appendix)
		context.appendix.Collections = make(map[string]collection)
		context.appendix.CommitMap = make(map[uint64]bool)

		enc := gob.NewEncoder(appendixFile)
		err = enc.Encode(context.appendix)

		appendixFile.Close()

		if err != nil {
			log.Fatal("Encode Error:", err)
		}
	}

}

func (context *Context) LoadAppendix() {
	var appendix appendix

	appendixFileName := getAppendixFilePath(context)

	//Open appendix file
	var appendixFile *os.File
	if _, err := os.Stat(appendixFileName); err == nil {
		appendixFile, err = os.OpenFile(appendixFileName, os.O_RDWR, 0660)
		if err != nil {
			fmt.Println(err)
			panic(err)
		}
	} else {
		panic(err)
	}

	dec := gob.NewDecoder(appendixFile)
	err := dec.Decode(&appendix)

	appendixFile.Close()

	if err != nil {
		log.Fatal("Decode Error:", err)
	}

	context.appendix = appendix
}

func (appendix *appendix) saveAppendix(context *Context) {
	appendixFileName := getAppendixFilePath(context)

	//Open appendix file
	var appendixFile *os.File
	if _, err := os.Stat(appendixFileName); err == nil {
		appendixFile, err = os.OpenFile(appendixFileName, os.O_RDWR, 0660)
		if err != nil {
			fmt.Println(err)
			panic(err)
		}
	} else {
		panic(err)
	}

	enc := gob.NewEncoder(appendixFile)
	err := enc.Encode(appendix)

	appendixFile.Close()

	if err != nil {
		log.Fatal("Encode Error:", err)
	}
}

func getAppendixFilePath(context *Context) string {
	appendixFileName := context.Config.Locations.Data
	if strings.HasSuffix(appendixFileName, "/") {
		appendixFileName = appendixFileName + "appendix"
	} else {
		appendixFileName = appendixFileName + "/appendix"
	}

	return appendixFileName
}

func NewContext(config *Config) *Context {
	context := new(Context)
	context.Config = *config
	context.blobstore = new(DiskStore)
	return context
}
