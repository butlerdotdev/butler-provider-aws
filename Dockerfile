# Butler Provider AWS Controller
FROM --platform=$BUILDPLATFORM golang:1.24-alpine AS builder
ARG TARGETOS
ARG TARGETARCH
ARG REPO_DIR=.
WORKDIR /workspace
RUN apk add --no-cache git make

# Copy butler-api first (for replace directive)
COPY butler-api/go.mod butler-api/go.sum ./butler-api/
COPY butler-api/api/ ./butler-api/api/

# Copy provider module and download deps
COPY ${REPO_DIR}/go.mod ${REPO_DIR}/go.sum ./${REPO_DIR}/
WORKDIR /workspace/${REPO_DIR}
RUN go mod download

# Copy provider source and build
COPY ${REPO_DIR}/ /workspace/${REPO_DIR}/
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -ldflags="-s -w" -o /manager ./cmd/

FROM gcr.io/distroless/static:nonroot
COPY --from=builder /manager /manager
USER 65532:65532
ENTRYPOINT ["/manager"]
