.PHONY: setup start delete stop start-producer start-consumer test

TOPIC_NAME ?= orders
PARTITIONS ?= 2
GROUP_ID ?= shipping

setup:
	npm i
	cd consumer && npm i
	cd producer && npm i
	docker compose up -d
# Loop until Kafka broker is ready
	@until docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list > /dev/null 2>&1; do \
		sleep 1; \
	done
	docker exec kafka /opt/kafka/bin/kafka-topics.sh \
		--bootstrap-server localhost:9092 \
		--create \
		--if-not-exists \
		--topic $(TOPIC_NAME) \
		--partitions $(PARTITIONS) \
		--replication-factor 1

start:
	docker compose up -d

delete:
	docker compose down

stop:
	docker compose stop

start-producer:
	cd producer && npm run start

start-consumer:
	cd consumer && node . ${GROUP_ID}

test:
	npm run test