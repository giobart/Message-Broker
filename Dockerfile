FROM golang:1.20

WORKDIR /app

COPY go.mod go.sum ./

RUN go mod download

RUN go get github.com/giobart/Message-Broker/pkg/broker

COPY examples/server/server.go ./

RUN CGO_ENABLED=0 go build -o /server

EXPOSE 9999

CMD ["/server -p 9999"]