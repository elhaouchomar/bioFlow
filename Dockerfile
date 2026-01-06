FROM golang:1.22-alpine

WORKDIR /app

# تثبيت Docker CLI لاستدعاء الحاويات الأخرى
RUN apk add --no-cache docker-cli

COPY go.mod go.sum ./
RUN go mod download

COPY . .

COPY templates ./templates

RUN go build -o pipeline main.go

CMD ["./pipeline"]