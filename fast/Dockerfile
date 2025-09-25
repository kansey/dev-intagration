FROM golang:1.17 AS builder

WORKDIR /app

# 1. Скопировать go.mod и go.sum и скачать зависимости
COPY app/go.mod app/go.sum ./
RUN go mod download

# 2. Скопировать исходники
#COPY app/*.go ./
#COPY app/producers ./producers
#COPY app/metrics ./metrics

COPY app/. ./

# 3. Собрать бинарь
RUN go build -v -o app .

# --- финальный контейнер ---
FROM debian:bullseye-slim

WORKDIR /app
COPY --from=builder /app/app .

#CMD ["./app"]
ENTRYPOINT ["./app"]
