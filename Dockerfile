FROM golang:1.16

WORKDIR /app

COPY go.mod go.sum ./

RUN go mod download

COPY . ./

RUN go get -u

RUN go build -o /server github.com/giobart/message-broker/examples/server

EXPOSE 9999

CMD ["sh","-c","/server -p 9999"]