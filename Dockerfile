# Build Stage
FROM golang:1.26-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN go build -o /praetor-consumer .

# Run Stage
FROM alpine:3.23@sha256:fd791d74b68913cbb027c6546007b3f0d3bc45125f797758156952bc2d6daf40

WORKDIR /

COPY --from=builder /praetor-consumer /praetor-consumer

USER 1000:1000

CMD ["/praetor-consumer"]
