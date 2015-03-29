package db

type pointer string

type collection struct {
	Name             string
	DocumentPointers map[string]pointer
}

type document struct {
	Data    string
	Id      string
	Pointer pointer
}
