FROM golang:1.24-alpine

WORKDIR /server
COPY . .
RUN go build

CMD ["/server/node"]
