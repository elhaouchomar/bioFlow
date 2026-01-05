FROM golang:1.22-alpine

WORKDIR /app

RUN apk add --no-cache docker-cli

COPY go.mod ./
RUN go mod download

COPY . .

RUN go build -o pipeline

CMD ["./pipeline"]
