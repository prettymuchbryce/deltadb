package net

import (
	"deltadb/src/db"
	"strconv"
)

type DeleteCollectionRequest struct {
	OpCode         byte
	RequestId      uint32
	Source         string
	Message        string
	CollectionName string
}

type DeleteCollectionResponse struct {
	OpCode    byte
	RequestId uint32
	Success   bool
	Error     string
	Hash      uint64
}

func (request *DeleteCollectionRequest) GetOpCode() byte {
	return request.OpCode
}

func (request *DeleteCollectionRequest) PerformOperation(context *db.Context) NetMessage {
	hash, err := db.DeleteCollection(context, request.Source, request.Message, request.CollectionName)
	deleteCollectionResponse := new(DeleteCollectionResponse)
	deleteCollectionResponse.OpCode = OpDeleteCollectionResponse
	deleteCollectionResponse.RequestId = request.RequestId
	deleteCollectionResponse.Hash = hash

	if err != nil {
		deleteCollectionResponse.Success = false
		deleteCollectionResponse.Error = err.Error()
	} else {
		deleteCollectionResponse.Success = true
	}

	return deleteCollectionResponse
}

func (response *DeleteCollectionResponse) String() string {
	return "success: " + strconv.FormatBool(response.Success) + "\nhash:" + db.UintHashToHex(response.Hash)
}

func (response *DeleteCollectionResponse) GetOpCode() byte {
	return response.OpCode
}
