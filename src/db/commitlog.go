package db

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"encoding/hex"
	"hash/fnv"
	"time"
)

const longForm = "Jan 2, 2006 at 3:04pm (MST)"

type Commit struct {
	Source    string
	Message   string
	Time      int64
	Operation Operation
	Hash      uint64
}

type Operation struct {
	Type           OperationType
	CollectionName string
	Id             string
	Pointer        pointer
	Hash           uint64
}

func newCommit(message string, source string, operation Operation) (error error, c *Commit) {
	commit := new(Commit)
	commit.Time = time.Now().Unix()
	commit.Message = message
	commit.Source = source
	commit.Operation = operation

	err, hash := commit.getHash()
	if err != nil {
		return err, nil
	}

	commit.Hash = hash

	return nil, commit
}

func HexHashToUint(hash string) (error, uint64) {
	bs, err := hex.DecodeString(hash)
	if err != nil {
		return err, 0
	}
	uintHash, _ := binary.Uvarint(bs)
	return nil, uintHash
}

func UintHashToHex(hash uint64) string {
	bs := make([]byte, binary.MaxVarintLen64)
	binary.PutUvarint(bs, hash)
	hashString := hex.EncodeToString(bs)
	return hashString
}

func (commit *Commit) getHash() (error error, hash uint64) {
	commitBuffer := new(bytes.Buffer)
	enc := gob.NewEncoder(commitBuffer)
	err := enc.Encode(commit)

	if err != nil {
		return err, 0
	}

	fnvHash := fnv.New64a()
	fnvHash.Write(commitBuffer.Bytes())
	return nil, fnvHash.Sum64()
}

func (commit Commit) String() string {
	hashString := UintHashToHex(commit.Hash)

	return hashString + " " + commit.Source + " " + commit.Message + " " + time.Unix(commit.Time, 0).Format(longForm) + " " + commit.Operation.String()
}

func (operation Operation) String() string {
	return operation.Type.String() + " " + operation.CollectionName + " " + operation.Id
}

func (o OperationType) String() string {
	switch o {
	case 0:
		return "OpCreateCollection"
	case 1:
		return "OpDeleteCollection"
	case 2:
		return "OpDeleteDocument"
	case 3:
		return "OpSetDocument"
	case 4:
		return "OpRollback"
	}
	return ""
}

type OperationType int

const (
	OpCreateCollection OperationType = iota
	OpDeleteCollection
	OpDeleteDocument
	OpSetDocument
	OpRollback
)
