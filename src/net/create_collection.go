package net

import (
	"deltadb/src/db"
	"strconv"
)

type CreateCollectionRequest struct {
	OpCode         byte
	RequestId      uint32
	Source         string
	Message        string
	CollectionName string
}

type CreateCollectionResponse struct {
	OpCode    byte
	RequestId uint32
	Success   bool
	Error     string
	Hash      uint64
}

func (request *CreateCollectionRequest) GetOpCode() byte {
	return request.OpCode
}

func (request *CreateCollectionRequest) PerformOperation(context *db.Context) NetMessage {
	hash, err := db.CreateCollection(context, request.Source, request.Message, request.CollectionName)
	createCollectionResponse := new(CreateCollectionResponse)
	createCollectionResponse.OpCode = OpCreateCollectionResponse
	createCollectionResponse.RequestId = request.RequestId
	createCollectionResponse.Hash = hash

	if err != nil {
		createCollectionResponse.Success = false
		createCollectionResponse.Error = err.Error()
	} else {
		createCollectionResponse.Success = true
	}

	return createCollectionResponse
}

func (response *CreateCollectionResponse) String() string {
	return "success: " + strconv.FormatBool(response.Success)
}

func (response *CreateCollectionResponse) GetOpCode() byte {
	return response.OpCode
}
