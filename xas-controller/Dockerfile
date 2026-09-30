FROM golang:1.24-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY main.go ./
RUN CGO_ENABLED=0 GOOS=linux go build -o xas-controller .

FROM alpine:3.20
WORKDIR /root/
COPY --from=builder /app/xas-controller .
ENTRYPOINT ["./xas-controller"]
