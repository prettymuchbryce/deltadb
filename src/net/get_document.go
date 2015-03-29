package net

import (
	"deltadb/src/db"
	"strconv"
)

type GetDocumentRequest struct {
	OpCode         byte
	RequestId      uint32
	CollectionName string
	Id             string
	Hash           uint64
}

type GetDocumentResponse struct {
	OpCode    byte
	RequestId uint32
	Success   bool
	Error     string
	Document  string
}

func (request *GetDocumentRequest) GetOpCode() byte {
	return request.OpCode
}

func (request *GetDocumentRequest) PerformOperation(context *db.Context) NetMessage {
	if request.Hash == 0 {
		request.Hash = context.GetLatestHash()
	}

	err, val := db.GetDocument(context, request.CollectionName, request.Id, request.Hash)
	getDocumentResponse := new(GetDocumentResponse)
	getDocumentResponse.OpCode = OpGetDocumentResponse
	getDocumentResponse.RequestId = request.RequestId
	getDocumentResponse.Document = val

	if err != nil {
		getDocumentResponse.Success = false
		getDocumentResponse.Error = err.Error()
	} else {
		getDocumentResponse.Success = true
	}

	return getDocumentResponse
}

func (response *GetDocumentResponse) String() string {
	s := ""
	s += "success: " + strconv.FormatBool(response.Success)
	if response.Error != "" {
		s += "\nerror: " + response.Error
	}
	s += "\nDocument: " + response.Document
	return s
}

func (response *GetDocumentResponse) GetOpCode() byte {
	return response.OpCode
}
