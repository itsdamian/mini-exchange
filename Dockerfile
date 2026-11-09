# syntax=docker/dockerfile:1
# Multi-stage, multi-arch friendly: build natively on the builder platform and
# cross-compile for the target (docker buildx build --platform linux/amd64,linux/arm64).
FROM --platform=$BUILDPLATFORM golang:1.27 AS build
ARG TARGETOS TARGETARCH
ARG CMD=gateway
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -trimpath -ldflags="-s -w" -o /out/service ./cmd/${CMD}

# /data is created owned by the nonroot user so a mounted volume is writable
# (snapshots are written there).
RUN mkdir /data

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/service /service
COPY --from=build --chown=65532:65532 /data /data
USER nonroot:nonroot
EXPOSE 8080 50051
ENTRYPOINT ["/service"]
