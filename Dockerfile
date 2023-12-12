FROM golang:1.16
LABEL org.opencontainers.image.source https://github.com/giobart/message-broker/message-broker

WORKDIR /app

COPY go.mod go.sum ./

RUN go mod download

COPY . ./

RUN go build -o /server examples/server/server.go

EXPOSE 9999

CMD ["sh","-c","/server -p 9999"]
