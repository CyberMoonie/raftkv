FROM golang:1.27-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /raftkv ./cmd/raftkv && mkdir /data

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /raftkv /raftkv
COPY --from=build --chown=nonroot:nonroot /data /data
VOLUME /data
ENTRYPOINT ["/raftkv"]
