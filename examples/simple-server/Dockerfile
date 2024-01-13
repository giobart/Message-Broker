FROM golang:1.21
LABEL org.opencontainers.image.source https://github.com/giobart/message-broker/simple-message-broker

WORKDIR /app

COPY go.mod go.sum ./

RUN go mod download

COPY . ./

RUN go build -o /server server.go

EXPOSE 9999

CMD ["sh","-c","/server"]
