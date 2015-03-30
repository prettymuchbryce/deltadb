package main

import (
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"

	"github.com/mattn/go-shellwords"
	db "github.com/prettymuchbryce/deltadb/src/db"
	deltadbnet "github.com/prettymuchbryce/deltadb/src/net"
)

const cliSource = "cli"

var commandList []string
var connection net.Conn

func main() {
	fmt.Println("")
	fmt.Println("Welcome to deltadb 0.0.1")
	fmt.Println("Type `help;` for a list of commands")
	fmt.Println("")
	for {
		var command string
		fmt.Scan(&command)

		commandList = append(commandList, command)

		if command[len(command)-1:] == string(";") {
			processInput(commandList)
			commandList = *new([]string)
		}
	}
}

func connect(host string, port string) net.Conn {
	conn, err := net.Dial("tcp", host+":"+port)
	if err != nil {
		fmt.Println(err)
		return nil
	}
	fmt.Println("connected")
	return conn
}

func handleMessage(conn net.Conn, msg []byte, opCode byte) {
	netMessage, err := deltadbnet.GetNetMessageByOpCode(msg, opCode)
	if err != nil {
		fmt.Println("Received malformed response.")
		return
	}

	netResponse, success := netMessage.(deltadbnet.NetResponse)
	if !success {
		fmt.Println("Received malformed response.")
		return
	}

	fmt.Println(netResponse.String())
}

func listen(conn net.Conn) {
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
			//Send error message
			break
		} else {
			handleMessage(conn, messageBytes, opCodeBytes[0])
		}
	}
}

func getCollectionNameAndKeyName(s string) (collectionName string, keyName string, error error) {
	var keyStartIndex int
	var keyEndIndex int

	for i := 0; i < len(s); i++ {
		if string(s[i]) == "[" {
			keyStartIndex = i
		} else if string(s[i]) == "]" {
			keyEndIndex = i
		} else {
			if keyStartIndex != 0 {
				keyName += string(s[i])
			} else {
				collectionName += string(s[i])
			}
		}
	}

	if keyStartIndex == 0 || keyEndIndex == 0 {
		return "", "", errors.New("Invalid key and collection name")
	}

	return collectionName, keyName, nil
}

func processInput(commandList []string) {
	//Remove semicolon
	lastItem := commandList[len(commandList)-1]
	if len(lastItem) > 1 {
		commandList[len(commandList)-1] = lastItem[0 : len(lastItem)-1]
	}

	commandString := strings.Join(commandList, " ")
	commandList, err := shellwords.Parse(commandString)
	if err != nil {
		fmt.Println(err)
		return
	}

	switch commandList[0] {
	case "help":
		fmt.Println("")
		fmt.Println("Command List")
		fmt.Println("All expressions end with a semicolon")
		fmt.Println("------")
		fmt.Println("connect;")
		fmt.Println("listcollections;")
		fmt.Println("list collection;                 #e.g. list users;")
		fmt.Println("listhistory limit;               #e.g. listhistory 10;")
		fmt.Println("get collection[key];             #e.g. get users[0];")
		fmt.Println("createcollection msg name;       #e.g. createcollection \"creating users\" users;")
		fmt.Println("deletecollection msg name;       #e.g. deletecollection \"deleting users\" users;")
		fmt.Println("set msg collection[key] = value; #e.g. set \"creating user\"  users[0] = '{ \"name\": \"Bryce\" }';")
		fmt.Println("del msg collection[key];         #e.g. del \"deleting user\" users[0];")
		fmt.Println("rollback msg hash;               #e.g. rollback \"reverting issue\" b9f8a7c0f9a18db20900;")
		fmt.Println("")
		break
	case "connect":
		connection = connect("localhost", "8012")
		go listen(connection)
		break
	case "get":
		if len(commandList) != 2 {
			fmt.Println("Invalid expression.\nUsage: get collection[key];")
			return
		}
		request := new(deltadbnet.GetDocumentRequest)
		request.RequestId = deltadbnet.GetNewRequestId()
		request.OpCode = deltadbnet.OpGetDocumentRequest

		collection, key, err := getCollectionNameAndKeyName(commandList[1])
		if err != nil {
			fmt.Println("Malformed Request")
			return
		}
		request.CollectionName = collection
		request.Id = key
		err = deltadbnet.PackAndSendMessage(connection, request)
		if err != nil {
			fmt.Println(err)
			return
		}
		break
	case "set":
		if len(commandList) != 5 {
			fmt.Println("Invalid expression.\nUsage: set message collection[key] = value;")
			return
		}
		request := new(deltadbnet.SetDocumentRequest)
		request.RequestId = deltadbnet.GetNewRequestId()
		request.OpCode = deltadbnet.OpSetDocumentRequest
		request.Source = cliSource
		request.Message = commandList[1]

		collection, key, err := getCollectionNameAndKeyName(commandList[2])
		if err != nil {
			fmt.Println(err)
			return
		}

		request.CollectionName = collection
		request.Id = key
		request.Data = commandList[4]

		err = deltadbnet.PackAndSendMessage(connection, request)
		if err != nil {
			fmt.Println(err)
			return
		}
		break
	case "del":
		if len(commandList) != 2 {
			fmt.Println("Invalid expression.\nUsage: del collection[key];")
			return
		}
		request := new(deltadbnet.DeleteDocumentRequest)
		request.RequestId = deltadbnet.GetNewRequestId()
		request.OpCode = deltadbnet.OpDeleteDocumentRequest

		collection, key, err := getCollectionNameAndKeyName(commandList[1])
		if err != nil {
			fmt.Println("Malformed Request")
			return
		}
		request.CollectionName = collection
		request.Id = key
		err = deltadbnet.PackAndSendMessage(connection, request)
		if err != nil {
			fmt.Println(err)
			return
		}
		break
	case "createcollection":
		if len(commandList) != 3 {
			fmt.Println("Invalid expression.\nUsage: createcollection message collectionName;")
			return
		}
		request := new(deltadbnet.CreateCollectionRequest)
		request.RequestId = deltadbnet.GetNewRequestId()
		request.OpCode = deltadbnet.OpCreateCollectionRequest
		request.Source = cliSource
		request.CollectionName = commandList[2]
		request.Message = commandList[1]
		err := deltadbnet.PackAndSendMessage(connection, request)
		if err != nil {
			fmt.Println(err)
			return
		}
		break
	case "deletecollection":
		if len(commandList) != 3 {
			fmt.Println("Invalid expression.\nUsage: deletecollection message collection;")
			return
		}
		request := new(deltadbnet.DeleteCollectionRequest)
		request.RequestId = deltadbnet.GetNewRequestId()
		request.OpCode = deltadbnet.OpDeleteCollectionRequest
		request.CollectionName = commandList[2]
		request.Message = commandList[1]
		err = deltadbnet.PackAndSendMessage(connection, request)
		if err != nil {
			fmt.Println(err)
			return
		}
		break
	case "listcollections":
		if len(commandList) != 1 {
			fmt.Println("Invalid expression.\nUsage: listcollections;")
			return
		}
		request := new(deltadbnet.ListCollectionsRequest)
		request.RequestId = deltadbnet.GetNewRequestId()
		request.OpCode = deltadbnet.OpListCollectionsRequest
		err := deltadbnet.PackAndSendMessage(connection, request)
		if err != nil {
			fmt.Println(err)
			return
		}
		break
	case "list":
		if len(commandList) != 2 {
			fmt.Println("Invalid expression.\nUsage: list collectionname;")
			return
		}
		request := new(deltadbnet.ListDocumentsRequest)
		request.RequestId = deltadbnet.GetNewRequestId()
		request.OpCode = deltadbnet.OpListDocumentsRequest
		request.CollectionName = commandList[1]
		err = deltadbnet.PackAndSendMessage(connection, request)
		if err != nil {
			fmt.Println(err)
			return
		}
		break
	case "listhistory":
		if len(commandList) != 2 {
			fmt.Println("Invalid expression.\nUsage: listhistory limit;")
			return
		}
		request := new(deltadbnet.ListHistoryRequest)
		request.RequestId = deltadbnet.GetNewRequestId()
		request.OpCode = deltadbnet.OpListHistoryRequest
		request.Limit, err = strconv.Atoi(commandList[1])
		if err != nil {
			fmt.Println(err)
			return
		}
		err = deltadbnet.PackAndSendMessage(connection, request)
		if err != nil {
			fmt.Println(err)
			return
		}
		break
	case "rollback":
		if len(commandList) != 3 {
			fmt.Println("Invalid expression.\nUsage: rollback message hash;")
			return
		}
		request := new(deltadbnet.RollbackRequest)
		request.RequestId = deltadbnet.GetNewRequestId()
		request.OpCode = deltadbnet.OpRollbackRequest
		request.Message = commandList[1]
		err, hash := db.HexHashToUint(commandList[2])
		if err != nil {
			fmt.Println("Malformed Request")
			return
		}
		request.Hash = hash
		err = deltadbnet.PackAndSendMessage(connection, request)
		if err != nil {
			fmt.Println(err)
			return
		}
		break
	default:
		fmt.Println("Unknown command:", commandList[0])
		break
	}
}
