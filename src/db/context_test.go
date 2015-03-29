package db

import (
	"log"
	"os"
	"path/filepath"
	"testing"
)

type testAppendix struct {
	Collections map[string]collection
	CommitMap   map[uint64]bool
	Commits     []Commit
}

func NewTestContext(t *testing.T) *Context {
	dir, err := filepath.Abs(filepath.Dir(os.Args[0]))
	if err != nil {
		log.Fatal(err)
	}

	context := new(Context)
	config := new(Config)
	config.Locations.Data = dir + "/testdata"

	context.Config = *config

	context.CreateDataDirIfDoesntExist()
	context.CreateAppendixIfDoesntExist()
	context.CreateBlobsDirIfDoesntExist()

	context.blobstore = new(DiskStore)
	appendix := *new(appendix)
	appendix.Collections = make(map[string]collection)
	appendix.CommitMap = make(map[uint64]bool)
	context.appendix = appendix

	return context
}
