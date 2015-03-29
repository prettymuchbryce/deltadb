package net

import (
	"deltadb/src/db"
	"encoding/binary"
	"fmt"
	"net"
)

func Start(context *db.Context) {
	server, err := net.Listen("tcp", ":"+context.Config.Server.Port)
	if err != nil {
		panic(err)
	}
	defer server.Close()
	for {
		conn, err := server.Accept()
		if err != nil {
			fmt.Println(err)
		}
		go handleConnection(context, conn)
	}
}

func sendGenericError(conn net.Conn, message string) {
	errorResponse := new(ErrorResponse)
	errorResponse.Message = message
	PackAndSendMessage(conn, errorResponse)
}

func handleConnection(context *db.Context, conn net.Conn) {
	defer conn.Close()

	for {
		lengthBytes := make([]byte, 8)
		_, err := conn.Read(lengthBytes)
		if err != nil {
			break
		}

		length, _ := binary.Uvarint(lengthBytes)

		opCodeBytes := make([]byte, 1)
		_, err = conn.Read(opCodeBytes)
		if err != nil {
			break
		}

		messageBytes := make([]byte, length)
		_, err = conn.Read(messageBytes)
		if err != nil {
			sendGenericError(conn, "Malformed message")
		} else {
			handleMessage(context, conn, messageBytes, opCodeBytes[0])
		}
	}
}

func handleMessage(context *db.Context, conn net.Conn, msg []byte, opCode byte) {
	netMessage, err := GetNetMessageByOpCode(msg, opCode)
	if err != nil {
		sendGenericError(conn, "Malformed message")
		return
	}

	netRequest, success := netMessage.(NetRequest)
	if !success {
		sendGenericError(conn, "Malformed message")
		return
	}

	response := netRequest.PerformOperation(context)
	PackAndSendMessage(conn, response)
}
