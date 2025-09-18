#!/bin/bash

# Отключаем автоматический экспорт переменных (необязательно, но рекомендуется)
set +a

# Получить директорию:
DIR=$PWD/docker/

# Загружаем переменные из .env файла
source $DIR/.env

# Включаем автоматический экспорт (если был включен ранее)
set -a

# Имя файла с дефолтовыми хостами для кафка
DEFAULT_FILE="default-kafka-hosts.txt"

# Имя файла, в который будем добавлять IP-адрес
FILE="kafka-hosts.txt"


# Создаем копию
echo "Создаем файла $FILE из файла $DEFAULT_FILE"
cp "$DIR/$DEFAULT_FILE" "$DIR/$FILE"

# Проверяем, получен ли IP-адрес
if [ -n "$IP_LOCAL_MACHINE" ]; then
    echo "Добавляем IP-адрес $IP_LOCAL_MACHINE в файл $FILE"
    # Добавляем IP-адрес в конец файла
    echo "$IP_LOCAL_MACHINE" >> "$DIR/$FILE"
else
    echo "Не удалось получить IP-адрес"
fi