package net

import (
	"deltadb/src/db"
	"strconv"
)

type ListCollectionsRequest struct {
	OpCode    byte
	RequestId uint32
	Hash      uint64
}

type ListCollectionsResponse struct {
	OpCode      byte
	RequestId   uint32
	Success     bool
	Collections []string
	Error       string
}

func (request *ListCollectionsRequest) GetOpCode() byte {
	return request.OpCode
}
func (request *ListCollectionsRequest) PerformOperation(context *db.Context) NetMessage {
	if request.Hash == 0 {
		request.Hash = context.GetLatestHash()
	}

	err, val := db.ListCollections(context, request.Hash)
	listCollectionsResponse := new(ListCollectionsResponse)
	listCollectionsResponse.OpCode = OpListCollectionsResponse
	listCollectionsResponse.RequestId = request.RequestId
	listCollectionsResponse.Collections = val

	if err != nil {
		listCollectionsResponse.Success = false
		listCollectionsResponse.Error = err.Error()
	} else {
		listCollectionsResponse.Success = true
	}

	return listCollectionsResponse
}

func (response *ListCollectionsResponse) String() string {
	s := "success: " + strconv.FormatBool(response.Success)
	for i := 0; i < len(response.Collections); i++ {
		s += "\n" + response.Collections[i]
	}
	return s
}

func (response *ListCollectionsResponse) GetOpCode() byte {
	return response.OpCode
}
