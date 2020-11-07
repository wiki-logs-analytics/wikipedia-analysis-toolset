
NAME    := connector
VERSION := 0.0.1

BIN_DIR     := ./bin

.PHONY: build_release

build_release:
	sudo apt-get install gccgo
	GOOS=linux go build -a -gccgoflags "-march=native -O3" -compiler gccgo -o ${BIN_DIR}/ ./lib/connector/src/*.go

build_debug:
	GOOS=linux go build -o ${BIN_DIR}/ ./lib/connector/src/*.go

run: build_release
	${BIN_DIR}/${NAME} ${flags}

lint:
	golangci-lint run ./...

test:
	go test -cover ./...  -coverprofile=coverage.out

coverage:
	go tool cover -html=coverage.out -o coverage.html

clean:
	go clean
	rm -f ${BIN_DIR}/${NAME}