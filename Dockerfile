FROM golang:latest

WORKDIR /usr/src/app

RUN apt-get update && apt-get install -y \
	gccgo \
	zip

ENV CGO_ENABLED 0
