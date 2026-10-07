# Build stage
FROM golang:1.24-alpine AS builder

WORKDIR /workspace

# Install SSL certificates and git if needed
RUN apk add --no-cache ca-certificates git

# Cache dependencies
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY cmd/ cmd/
COPY pkg/ pkg/

# Compile static binary
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o dynatrace-provider ./cmd/dynatrace-provider

# Runtime stage
FROM gcr.io/distroless/static-debian12:nonroot

WORKDIR /
COPY --from=builder /workspace/dynatrace-provider /dynatrace-provider
USER 65532:65532

ENTRYPOINT ["/dynatrace-provider"]
