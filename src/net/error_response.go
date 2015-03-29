package net

type ErrorResponse struct {
	OpCode    byte
	RequestId uint32
	Message   string
}

func (response *ErrorResponse) GetOpCode() byte {
	return response.OpCode
}

func (response *ErrorResponse) String() string {
	return "message: " + response.Message
}
