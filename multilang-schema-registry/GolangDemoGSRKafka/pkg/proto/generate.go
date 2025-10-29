package proto

//go:generate protoc --proto_path=../../protos --go_out=. --go_opt=paths=source_relative user.proto
//go:generate protoc --proto_path=../../protos --go_out=. --go_opt=paths=source_relative order.proto
