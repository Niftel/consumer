# Build Stage
FROM golang:1.26-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN go build -o /praetor-consumer .

# Run Stage
FROM alpine:3.19

WORKDIR /

COPY --from=builder /praetor-consumer /praetor-consumer

USER 1000:1000

CMD ["/praetor-consumer"]
