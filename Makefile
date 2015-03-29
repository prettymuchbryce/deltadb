test:
	go test ./src/... -v
cli:
	go run ./cmd/cli/main.go
run:
	go run ./src/main.go --config ./deltad.conf