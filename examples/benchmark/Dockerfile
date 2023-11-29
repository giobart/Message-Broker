FROM golang:1.16

WORKDIR /app

COPY go.mod go.sum ./

RUN go mod download

COPY . ./

RUN go build -o benchmark

EXPOSE 10000

CMD ["sh","-c","/benchmark -a 0.0.0.0:9999"]