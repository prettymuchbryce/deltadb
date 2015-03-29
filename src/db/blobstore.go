package db

import (
	"encoding/binary"
	"encoding/gob"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/fnv"
	"log"
	"os"
	"strings"
)

type BlobStore interface {
	storeBlob(*Context, string) pointer
	getBlob(*Context, pointer) (error, string)
}

type DiskStore struct{}

func (context *Context) CreateBlobsDirIfDoesntExist() {
	blobsDirName := context.Config.Locations.Data
	if strings.HasSuffix(blobsDirName, "/") {
		blobsDirName = blobsDirName + "blobs/"
	} else {
		blobsDirName = blobsDirName + "/blobs/"
	}

	if _, err := os.Stat(blobsDirName); err != nil {
		err := os.Mkdir(blobsDirName, 0770)
		if err != nil {
			panic(err)
		}
	}
}

func (diskStore *DiskStore) storeBlob(context *Context, value string) pointer {
	blobsDirName := context.Config.Locations.Data
	if strings.HasSuffix(blobsDirName, "/") {
		blobsDirName = blobsDirName + "blobs/"
	} else {
		blobsDirName = blobsDirName + "/blobs/"
	}

	hash := fnv.New64a()
	hash.Write([]byte(value))

	bs := make([]byte, binary.MaxVarintLen64)
	binary.PutUvarint(bs, hash.Sum64())
	fileName := hex.EncodeToString(bs)

	blobFile, err := os.Create(blobsDirName + fileName)
	if err != nil {
		panic(err)
	}

	enc := gob.NewEncoder(blobFile)
	err = enc.Encode(value)

	blobFile.Close()

	if err != nil {
		log.Fatal("Encode Error:", err)
	}

	return pointer(fileName)
}

func (diskStore *DiskStore) getBlob(context *Context, pointer pointer) (e error, value string) {
	blobsDirName := context.Config.Locations.Data
	if strings.HasSuffix(blobsDirName, "/") {
		blobsDirName = blobsDirName + "blobs/"
	} else {
		blobsDirName = blobsDirName + "/blobs/"
	}

	blobFileName := blobsDirName + string(pointer)

	if _, err := os.Stat(blobFileName); err == nil {
		blobFile, err := os.OpenFile(blobFileName, os.O_RDWR, 0660)
		if err != nil {
			fmt.Println(err)
			panic(err)
		}

		dec := gob.NewDecoder(blobFile)
		err = dec.Decode(&value)

		blobFile.Close()

		return nil, value
	} else {
		return errors.New("No such blob"), ""
	}
}
