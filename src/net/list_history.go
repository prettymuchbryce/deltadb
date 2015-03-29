package net

import (
	"deltadb/src/db"
	"strconv"
)

type ListHistoryRequest struct {
	OpCode    byte
	RequestId uint32
	Limit     int
	Hash      uint64
}

type ListHistoryResponse struct {
	OpCode    byte
	RequestId uint32
	Success   bool
	Error     string
	Commits   []db.Commit
}

func (request *ListHistoryRequest) GetOpCode() byte {
	return request.OpCode
}

func (request *ListHistoryRequest) PerformOperation(context *db.Context) NetMessage {
	if request.Hash == 0 {
		request.Hash = context.GetLatestHash()
	}

	err, val := db.ListHistory(context, request.Limit, request.Hash)
	listHistoryResponse := new(ListHistoryResponse)
	listHistoryResponse.OpCode = OpListHistoryResponse
	listHistoryResponse.RequestId = request.RequestId
	listHistoryResponse.Commits = val

	if err != nil {
		listHistoryResponse.Success = false
		listHistoryResponse.Error = err.Error()
	} else {
		listHistoryResponse.Success = true
	}

	return listHistoryResponse
}

func (response *ListHistoryResponse) String() string {
	s := "success: " + strconv.FormatBool(response.Success)
	for i := 0; i < len(response.Commits); i++ {
		s += "\n" + response.Commits[i].String()
	}
	return s
}

func (response *ListHistoryResponse) GetOpCode() byte {
	return response.OpCode
}
