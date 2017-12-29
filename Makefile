

NAME = kelipsd

clean:
	rm -v $(NAME)

deps:
	go get -d -v .

kelipsd:
	go build -o kelipsd examples/main.go

test:
	go test -race -cover .

protoc:
	protoc structs.proto -I ./ -I ../../../ --go_out=plugins=grpc:.
