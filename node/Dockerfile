FROM golang:1.21-alpine

WORKDIR /server
COPY . .
RUN go build

CMD ["/server/node"]
