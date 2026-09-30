FROM golang:alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY main.go dashboard.html ./
RUN CGO_ENABLED=0 GOOS=linux go build -o mock-dynatrace-server .

FROM alpine:3.20
RUN apk add --no-cache ca-certificates
WORKDIR /root/
COPY --from=builder /app/mock-dynatrace-server .
EXPOSE 8080
ENTRYPOINT ["./mock-dynatrace-server"]
