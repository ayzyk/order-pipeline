# One image with all four binaries, Kubernetes picks the command per Deployment.

FROM golang:1.25-alpine AS build
WORKDIR /src

# dependencies first, so this layer is cached until go.mod changes
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/ ./cmd/...

# distroless: no shell and no package manager, so there is less to attack
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/ /app/
# numeric uid, otherwise Kubernetes can not check runAsNonRoot
USER 65532:65532
EXPOSE 8080
ENTRYPOINT ["/app/api"]
