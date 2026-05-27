FROM golang:1.25-alpine

WORKDIR /server
COPY go.mod go.mod
COPY go.sum go.sum

RUN go mod download

RUN apk add --no-cache gcc musl-dev

COPY . .

RUN CGO_ENABLED=1 go build -race

CMD ["/server/node"]
