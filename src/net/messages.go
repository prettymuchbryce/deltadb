package net

import (
	"deltadb/src/db"
	"encoding/binary"
	"errors"
	"math/rand"
	"net"

	"github.com/fatih/structs"
	"gopkg.in/vmihailenco/msgpack.v2"
)

type NetMessage interface {
	GetOpCode() byte
}

type NetResponse interface {
	String() string
	GetOpCode() byte
}

type NetRequest interface {
	PerformOperation(context *db.Context) NetMessage
	GetOpCode() byte
}

const (
	OpCreateCollectionRequest byte = iota
	OpCreateCollectionResponse
	OpDeleteCollectionRequest
	OpDeleteCollectionResponse
	OpDeleteDocumentRequest
	OpDeleteDocumentResponse
	OpSetDocumentRequest
	OpSetDocumentResponse
	OpRollbackRequest
	OpRollbackResponse
	OpListCollectionsRequest
	OpListCollectionsResponse
	OpListDocumentsRequest
	OpListDocumentsResponse
	OpListHistoryRequest
	OpListHistoryResponse
	OpGetDocumentRequest
	OpGetDocumentResponse
	OpErrorResponse
)

func GetNewRequestId() uint32 {
	return rand.Uint32()
}

func GetNetMessageByOpCode(data []byte, opCode byte) (NetMessage, error) {
	var netMessage NetMessage

	switch opCode {
	case OpCreateCollectionRequest:
		netMessage = new(CreateCollectionRequest)
	case OpCreateCollectionResponse:
		netMessage = new(CreateCollectionResponse)
	case OpDeleteCollectionRequest:
		netMessage = new(DeleteCollectionRequest)
	case OpDeleteCollectionResponse:
		netMessage = new(DeleteCollectionResponse)
	case OpDeleteDocumentRequest:
		netMessage = new(DeleteDocumentRequest)
	case OpDeleteDocumentResponse:
		netMessage = new(DeleteDocumentResponse)
	case OpSetDocumentRequest:
		netMessage = new(SetDocumentRequest)
	case OpSetDocumentResponse:
		netMessage = new(SetDocumentResponse)
	case OpRollbackRequest:
		netMessage = new(RollbackRequest)
	case OpRollbackResponse:
		netMessage = new(RollbackResponse)
	case OpListCollectionsRequest:
		netMessage = new(ListCollectionsRequest)
	case OpListCollectionsResponse:
		netMessage = new(ListCollectionsResponse)
	case OpListDocumentsRequest:
		netMessage = new(ListDocumentsRequest)
	case OpListDocumentsResponse:
		netMessage = new(ListDocumentsResponse)
	case OpListHistoryRequest:
		netMessage = new(ListHistoryRequest)
	case OpListHistoryResponse:
		netMessage = new(ListHistoryResponse)
	case OpGetDocumentRequest:
		netMessage = new(GetDocumentRequest)
	case OpGetDocumentResponse:
		netMessage = new(GetDocumentResponse)
	case OpErrorResponse:
		netMessage = new(ErrorResponse)
	default:
		return nil, errors.New("Unknown messagetype for Opcode")
	}

	err := msgpack.Unmarshal(data, netMessage)
	if err != nil {
		return nil, err
	}

	return netMessage, nil
}

func CreateWireMessage(data []byte, opCode byte) []byte {
	d := *new([]byte)
	length := make([]byte, 8)
	binary.PutUvarint(length, uint64(len(data)))
	d = append(d, length...)
	d = append(d, opCode)
	d = append(d, data...)
	return d
}

func pack(data map[string]interface{}) ([]byte, error) {
	b, err := msgpack.Marshal(data)
	if err != nil {
		return nil, err
	}
	return b, nil
}

func PackAndSendMessage(conn net.Conn, message NetMessage) error {
	if conn == nil {
		return errors.New("No connection")
	}
	data, err := pack(structs.Map(message))
	if err != nil {
		return err
	}

	wireData := CreateWireMessage(data, message.GetOpCode())

	_, err = conn.Write(wireData)
	if err != nil {
		return err
	}

	return nil
}
