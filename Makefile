run: 
	go run cmd/api/main.go

test:
	go test -race ./...

compile:
	protoc cmd/api/v1/*.proto \
		--go_out=. \
		--go-grpc_out=. \
		--go_opt=paths=source_relative \
		--go-grpc_opt=paths=source_relative \
		--proto_path=.
