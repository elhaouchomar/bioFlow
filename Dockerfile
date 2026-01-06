FROM golang:1.22-alpine

WORKDIR /app

# Install Docker CLI to run bio-tools
RUN apk add --no-cache docker-cli

COPY go.mod ./
RUN go mod download

COPY . .

# IMPORTANT: Copy the templates folder
COPY templates ./templates

RUN go build -o pipeline

CMD ["./pipeline"]