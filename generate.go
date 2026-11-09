package ledgerclient

//go:generate protoc --go_out=. --go_opt=module=github.com/devotionalist/open-ledger-client-go --go-grpc_out=. --go-grpc_opt=module=github.com/devotionalist/open-ledger-client-go -I proto proto/bookie/v1/bookie.proto
