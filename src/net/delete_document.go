package net

import (
	"deltadb/src/db"
	"strconv"
)

type DeleteDocumentRequest struct {
	OpCode         byte
	RequestId      uint32
	Source         string
	Message        string
	CollectionName string
	Id             string
}

type DeleteDocumentResponse struct {
	OpCode    byte
	RequestId uint32
	Success   bool
	Error     string
	Hash      uint64
}

func (request *DeleteDocumentRequest) GetOpCode() byte {
	return request.OpCode
}

func (request *DeleteDocumentRequest) PerformOperation(context *db.Context) NetMessage {
	hash, err := db.DeleteDocument(context, request.Source, request.Message, request.CollectionName, request.Id)
	deleteDocumentResponse := new(DeleteDocumentResponse)
	deleteDocumentResponse.OpCode = OpDeleteDocumentResponse
	deleteDocumentResponse.RequestId = request.RequestId
	deleteDocumentResponse.Hash = hash

	if err != nil {
		deleteDocumentResponse.Success = false
		deleteDocumentResponse.Error = err.Error()
	} else {
		deleteDocumentResponse.Success = true
	}

	return deleteDocumentResponse
}

func (response *DeleteDocumentResponse) String() string {
	return "success: " + strconv.FormatBool(response.Success) + "\nhash:" + db.UintHashToHex(response.Hash)
}

func (response *DeleteDocumentResponse) GetOpCode() byte {
	return response.OpCode
}
