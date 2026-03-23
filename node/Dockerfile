FROM golang:1.24-alpine

WORKDIR /server
COPY go.mod go.mod
COPY go.sum go.sum

RUN go mod download

COPY . .

RUN go build

CMD ["/server/node"]
