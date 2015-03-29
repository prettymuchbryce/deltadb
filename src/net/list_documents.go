package net

import (
	"deltadb/src/db"
	"strconv"
)

type ListDocumentsRequest struct {
	OpCode         byte
	RequestId      uint32
	CollectionName string
	Hash           uint64
}

type ListDocumentsResponse struct {
	OpCode    byte
	RequestId uint32
	Error     string
	Success   bool
	Documents []string
}

func (request *ListDocumentsRequest) GetOpCode() byte {
	return request.OpCode
}

func (request *ListDocumentsRequest) PerformOperation(context *db.Context) NetMessage {
	if request.Hash == 0 {
		request.Hash = context.GetLatestHash()
	}

	err, val := db.ListDocuments(context, request.CollectionName, request.Hash)
	listDocumentsResponse := new(ListDocumentsResponse)
	listDocumentsResponse.OpCode = OpListDocumentsResponse
	listDocumentsResponse.RequestId = request.RequestId
	listDocumentsResponse.Documents = val

	if err != nil {
		listDocumentsResponse.Success = false
		listDocumentsResponse.Error = err.Error()
	} else {
		listDocumentsResponse.Success = true
	}

	return listDocumentsResponse
}

func (response *ListDocumentsResponse) String() string {
	s := "success: " + strconv.FormatBool(response.Success)
	for i := 0; i < len(response.Documents); i++ {
		s += "\n" + response.Documents[i]
	}
	return s
}

func (response *ListDocumentsResponse) GetOpCode() byte {
	return response.OpCode
}
