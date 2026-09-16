.PHONY: setup start delete stop start-producer start-consumer

TOPIC_NAME ?= orders
PARTITIONS ?= 2
GROUP_ID ?= shipping

setup:
	cd consumer && npm i
	cd producer && npm i
	docker-compose up -d
	docker exec kafka /opt/kafka/bin/kafka-topics.sh \
		--bootstrap-server localhost:9092 \
		--create \
		--if-not-exists \
		--topic $(TOPIC_NAME) \
		--partitions $(PARTITIONS) \
		--replication-factor 1

start:
	docker-compose up -d

delete:
	docker compose down

stop:
	docker compose stop

start-producer:
	cd producer && npm run start

start-consumer:
	cd consumer && node . ${GROUP_ID}