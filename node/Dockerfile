FROM golang:1.25-alpine

RUN apk add --no-cache gcc musl-dev

WORKDIR /server
COPY go.mod go.mod
COPY go.sum go.sum

RUN go mod download

COPY . .

RUN CGO_ENABLED=1 go build -race

CMD ["/server/node"]
