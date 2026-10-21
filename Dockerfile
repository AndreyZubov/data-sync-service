# syntax=docker/dockerfile:1

# --- build stage ---
# Runs on the build host's architecture and cross-compiles for the target, which
# is far faster than building under QEMU emulation for multi-arch images.
FROM --platform=$BUILDPLATFORM golang:1.27 AS build
WORKDIR /src

# Cache dependencies first so source edits do not invalidate the download layer.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

ARG VERSION=dev
ARG TARGETOS=linux
ARG TARGETARCH
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -trimpath \
        -ldflags="-s -w -X main.version=${VERSION}" \
        -o /out/syncd ./cmd/syncd

# --- runtime stage ---
FROM gcr.io/distroless/static-debian12:nonroot

ARG VERSION=dev
LABEL org.opencontainers.image.title="data-sync-service" \
      org.opencontainers.image.description="Go service for bulk data synchronisation between Salesforce and PostgreSQL" \
      org.opencontainers.image.source="https://github.com/AndreyZubov/data-sync-service" \
      org.opencontainers.image.licenses="MIT" \
      org.opencontainers.image.version="${VERSION}"

WORKDIR /
COPY --from=build /out/syncd /syncd
EXPOSE 8080
USER nonroot:nonroot
ENTRYPOINT ["/syncd"]
