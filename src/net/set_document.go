package net

import (
	"deltadb/src/db"
	"strconv"
)

type SetDocumentRequest struct {
	OpCode         byte
	RequestId      uint32
	Source         string
	Message        string
	CollectionName string
	Id             string
	Data           string
}

type SetDocumentResponse struct {
	OpCode    byte
	RequestId uint32
	Success   bool
	Error     string
	Hash      uint64
}

func (request *SetDocumentRequest) GetOpCode() byte {
	return request.OpCode
}

func (request *SetDocumentRequest) PerformOperation(context *db.Context) NetMessage {
	hash, err := db.SetDocument(context, request.Source, request.Message, request.CollectionName, request.Id, request.Data)
	setDocumentResponse := new(SetDocumentResponse)
	setDocumentResponse.OpCode = OpSetDocumentResponse
	setDocumentResponse.RequestId = request.RequestId
	setDocumentResponse.Hash = hash

	if err != nil {
		setDocumentResponse.Success = false
		setDocumentResponse.Error = err.Error()
	} else {
		setDocumentResponse.Success = true
	}

	return setDocumentResponse
}

func (response *SetDocumentResponse) String() string {
	return "success: " + strconv.FormatBool(response.Success) + "\nhash:" + db.UintHashToHex(response.Hash)
}

func (response *SetDocumentResponse) GetOpCode() byte {
	return response.OpCode
}
