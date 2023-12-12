FROM golang:1.16
LABEL org.opencontainers.image.source https://github.com/giobart/message-broker/broker-benchmark

WORKDIR /app

COPY go.mod go.sum ./

RUN go mod download

COPY . ./

RUN go build -o benchmark

EXPOSE 10000

CMD ["sh","-c","/benchmark -a 0.0.0.0:9999"]