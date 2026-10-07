FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o /out/server ./cmd/server \
 && CGO_ENABLED=0 go build -ldflags="-s -w" -o /out/loadtest ./cmd/loadtest

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/server /out/loadtest /
EXPOSE 8080
ENTRYPOINT ["/server"]
