# Build Stage — compile on the native CI runner instead of emulating the target.
FROM --platform=$BUILDPLATFORM golang:1.26-alpine AS builder

ARG TARGETOS
ARG TARGETARCH

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -o /praetor-consumer .

# Run Stage
FROM alpine:3.23@sha256:fd791d74b68913cbb027c6546007b3f0d3bc45125f797758156952bc2d6daf40

WORKDIR /

COPY --from=builder /praetor-consumer /praetor-consumer

USER 1000:1000

CMD ["/praetor-consumer"]
