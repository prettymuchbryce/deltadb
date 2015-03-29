package net

import (
	"deltadb/src/db"
	"strconv"
)

type RollbackRequest struct {
	OpCode    byte
	RequestId uint32
	Source    string
	Message   string
	Hash      uint64
}

type RollbackResponse struct {
	OpCode    byte
	RequestId uint32
	Success   bool
	Error     string
	Hash      uint64
}

func (request *RollbackRequest) GetOpCode() byte {
	return request.OpCode
}

func (request *RollbackRequest) PerformOperation(context *db.Context) NetMessage {
	if request.Hash == 0 {
		request.Hash = context.GetLatestHash()
	}

	hash, err := db.Rollback(context, request.Source, request.Message, request.Hash)
	rollbackResponse := new(RollbackResponse)
	rollbackResponse.OpCode = OpRollbackResponse
	rollbackResponse.RequestId = request.RequestId
	rollbackResponse.Hash = hash

	if err != nil {
		rollbackResponse.Success = false
		rollbackResponse.Error = err.Error()
	} else {
		rollbackResponse.Success = true
	}

	return rollbackResponse
}

func (response *RollbackResponse) String() string {
	return "success: " + strconv.FormatBool(response.Success) + "\nhash:" + db.UintHashToHex(response.Hash)
}

func (response *RollbackResponse) GetOpCode() byte {
	return response.OpCode
}
