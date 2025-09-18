## Описание.

Смотрела материалы по теме. На Docker Hub самые популярные сборки:
1. https://hub.docker.com/layers/bitnami/kafka/3.9.0/images/sha256-3214f5fbbbec6ed46f97ec2cca848853768ba649282c87bda5c6386cce632dbc
Репозиторий https://github.com/bitnami/containers/blob/main/bitnami/kafka/docker-compose-cluster.yml


2. https://hub.docker.com/layers/apache/kafka/3.9.1/images/sha256-5862db4a63a6dd7d46fd14771b10a1b39e069c2c47f17d8e4640f960720a0ead
Документация:
- docer-composer для single-node ssl https://github.com/apache/kafka/blob/3.9.1/docker/examples/docker-compose-files/single-node/ssl/docker-compose.yml
- docer-composer для cluster ssl https://github.com/apache/kafka/blob/3.9.1/docker/examples/docker-compose-files/cluster/combined/ssl/docker-compose.yml


Пыталась реализовать как в статье, т.к. там есть генерация сертификатов:
- https://habr.com/ru/articles/810061/ (минус В представленной конфигурации настроены SASL, SSL, ACL. Много параметров и основана на KRaft)
- https://jaehyeon.me/blog/2023-07-06-kafka-development-with-docker-part-9/ (основана на zookeeper. Пока не понятно, что нам надо zookeeper или KRaft)

Они основаны на сборке bitnami/kafka
Пока решила остановить на варианте с KRaft и кластером
В Makefile реализовала:
- инициализация фала .env
- создание файла kafka-hosts
- генерация ключей
- поднятие вм с go

## Начало работы

* Настроить переменные в файле ./docker/.env.example

* Сгенерировать сертификаты:

```
make generate-cert
```

Поднять вм c go
```
make up-local-backend
```

Долг:
Не разобралась с параметрами для kafka. Думаю нам надо не все эти параметры, чтоб осталось только ssl
