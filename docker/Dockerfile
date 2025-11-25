# =======================================================
#  STAGE 1 — Build Go binary
# =======================================================
FROM golang:1.25 AS builder

WORKDIR /app

# Install build tools
RUN apt-get update && apt-get install -y git build-essential

# Cache modules
COPY go.mod go.sum ./
RUN go mod download

# Copy source
COPY . .

# Build binary
RUN CGO_ENABLED=1 GOOS=linux GOARCH=amd64 go build -o notification-agent ./cmd/na

# =======================================================
#  STAGE 2 — Runtime image
# =======================================================
FROM alpine:3.20

WORKDIR /app

RUN apk add --no-cache sqlite-libs sqlite ca-certificates tzdata libc6-compat

COPY --from=builder /app/notification-agent /app/notification-agent

ENV NA_PORT=8081

EXPOSE 8081

CMD ["./notification-agent"]