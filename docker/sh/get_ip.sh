#!/bin/bash

# Отключаем автоматический экспорт переменных (необязательно, но рекомендуется)
set +a

# Загружаем переменные из .env файла
source .env

# Включаем автоматический экспорт (если был включен ранее)
set -a

# Имя файла с дефолтовыми хостами для кафка
DEFAULT_FILE="default-kafka-hosts.txt"

# Имя файла, в который будем добавлять IP-адрес
FILE="kafka-hosts.txt"

# Получить директорию:
DIR=$PWD/docker/

# Создаем копию
echo "Создаем файла $FILE из файла $DEFAULT_FILE"
cp "$DIR/$DEFAULT_FILE" "$DIR/$FILE"

# Получаем IP-адрес первого сетевого интерфейса (например, eth0, enpXsX)
# Команда ip a | grep "inet " | awk '{print $2}' | cut -d/ -f1
# Эта команда ищет строки с 'inet', извлекает второй элемент (сам IP-адрес),
# а затем удаляет подсеть (/24).
CURRENT_IP=$(ip a | grep "inet " | awk '{print $2}' | cut -d/ -f1)

# Проверяем, получен ли IP-адрес
if [ -n "$CURRENT_IP" ]; then
    echo "Добавляем IP-адрес $CURRENT_IP в файл $FILE"
    # Добавляем IP-адрес в конец файла
    echo "$CURRENT_IP" >> "$DIR/$FILE"
else
    echo "Не удалось получить IP-адрес"
fi