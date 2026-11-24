FROM golang:1.25-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/ ./cmd/...

FROM alpine:3.20
COPY --from=build /out/ /usr/local/bin/
VOLUME /data
EXPOSE 7000 9100
ENTRYPOINT ["raftkv"]
