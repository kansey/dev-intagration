# Подготовить локально файл .env, сперва 
init-local-env:
	./docker/sh/init-local-env.sh

# Создаем файл kafka-hosts.txt
create-file-kafka-host: init-local-env
	./docker/sh/set_ip.sh 

# Генерация сертификатов
generate-cert: create-file-kafka-host
	 ./docker/sh/generate.sh 


# Поднять локально вм с go
up-local-backend:
	docker build -t cft-integration/backend-go-1.17 ${PWD}/backend/
	export COMPOSE_PROJECT_NAME=cft-integration
	docker run --name backend-go -it -v ${PWD}:/usr/src/app -w /usr/src/app -d cft-integration/backend-go-1.17

# Остановить и удалить локальную вм с go
down-local-backend:
	docker stop backend-go
	docker rm backend-go
