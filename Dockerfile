FROM golang:1.14-alpine AS build
ENV GO111MODULE=on
ENV CGO_ENABLED=0


COPY . /app
WORKDIR /app

RUN go build s3-backup.go

FROM alpine:latest

WORKDIR /

COPY --from=build /app .

ENTRYPOINT ./s3-backup
