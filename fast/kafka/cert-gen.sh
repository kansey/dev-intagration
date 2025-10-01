#!/bin/bash
set -e

PASSWORD="developer"
DAYS=365
BROKER_HOST="kafka"
BROKER_IP="127.0.0.1"
ALIAS="kafka"
CLIENT_ALIAS="client"

# Очистка старых файлов
rm -f kafka.keystore.jks kafka.truststore.jks ca.crt ca.key ca.srl \
      kafka.csr kafka.crt client.csr client.crt client.key client.p12

echo "👉 Генерация self-signed CA..."
openssl req -new -x509 -days $DAYS \
  -subj "/CN=MyKafkaCA/OU=Dev/O=MyCompany/L=City/S=State/C=US" \
  -keyout ca.key -out ca.crt -passout pass:$PASSWORD

echo "👉 Генерация keystore и ключа брокера..."
keytool -genkeypair \
  -alias $ALIAS \
  -keyalg RSA \
  -keysize 2048 \
  -dname "CN=$BROKER_HOST, OU=Dev, O=MyCompany, L=City, S=State, C=US" \
  -keystore kafka.keystore.jks \
  -storepass $PASSWORD \
  -keypass $PASSWORD

echo "👉 Создание CSR брокера..."
keytool -keystore kafka.keystore.jks -alias $ALIAS \
  -certreq -file kafka.csr -storepass $PASSWORD -keypass $PASSWORD

echo "👉 Создание openssl.cnf с SAN..."
cat > openssl.cnf <<EOF
[ req ]
distinguished_name = req_distinguished_name
req_extensions     = v3_req
prompt             = no

[ req_distinguished_name ]
CN = $BROKER_HOST

[ v3_req ]
keyUsage = keyEncipherment, digitalSignature
extendedKeyUsage = serverAuth, clientAuth
subjectAltName = @alt_names

[ alt_names ]
DNS.1 = $BROKER_HOST
DNS.2 = localhost
IP.1  = $BROKER_IP
IP.2  = 127.0.0.1
EOF

echo "👉 Подпись CSR брокера CA с SAN..."
openssl x509 -req -in kafka.csr -CA ca.crt -CAkey ca.key -CAcreateserial \
  -out kafka.crt -days $DAYS -passin pass:$PASSWORD \
  -extfile openssl.cnf -extensions v3_req

rm -f openssl.cnf

echo "👉 Импорт CA в truststore..."
keytool -import -trustcacerts -alias CARoot \
  -file ca.crt -keystore kafka.truststore.jks -storepass $PASSWORD -noprompt

echo "👉 Импорт CA в keystore..."
keytool -import -trustcacerts -alias CARoot \
  -file ca.crt -keystore kafka.keystore.jks -storepass $PASSWORD -noprompt

echo "👉 Импорт подписанного брокерского сертификата в keystore..."
keytool -import -alias $ALIAS -file kafka.crt \
  -keystore kafka.keystore.jks -storepass $PASSWORD -noprompt

echo "👉 Генерация клиентского ключа и CSR..."
openssl req -new -newkey rsa:2048 -days $DAYS \
  -nodes -keyout client.key -out client.csr \
  -subj "/CN=client/OU=Dev/O=MyCompany/L=City/S=State/C=US"

echo "👉 Подпись клиентского сертификата CA..."
openssl x509 -req -in client.csr -CA ca.crt -CAkey ca.key -CAcreateserial \
  -out client.crt -days $DAYS -passin pass:$PASSWORD

echo "👉 Экспорт клиента в PKCS12 (для Java/Sarama)..."
openssl pkcs12 -export -in client.crt -inkey client.key \
  -out client.p12 -name $CLIENT_ALIAS -passout pass:$PASSWORD

echo "✅ Готово!"
echo "Файлы: kafka.keystore.jks, kafka.truststore.jks, kafka.crt, ca.crt, client.crt, client.key, client.p12"
