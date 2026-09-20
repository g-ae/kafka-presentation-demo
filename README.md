# Kafka presentation Demo
Small Kafka use-case for a demo in the **302 Data infrastructures** class.

## Prerequisites
- Docker Engine
- Docker Compose
- NodeJS
- NPM

## Running demo
- Create Kafka broker and "orders" topic (starts with 2 partitions by default), setup npm packages for `consumer` and `producer` services
```bash
make
```

- Start Consumer that polls messages​ (note: default consumer group is shipping, you can put whatever name you like using flag `GROUP_ID`)
```bash
make start-consumer
```

- Start Producer that sends orders every 3 seconds​
```bash
make start-producer
```

- Stop broker​ (simulate broker crashing)
```bash
make stop
```

- Start broker again​
```bash
make start
```

- Open another consumer that will read all messages​
```bash
make start-consumer GROUP_ID=inventory
```

- Open second worker on `shipping` group for partition separation (horizontal scaling)​
```bash
make start-consumer
```

## Running Tests (Failsafe Semi-Live Demo)
Integration tests are configured to verify the Kafka broker in action and act as a failsafe semi-live demo.

Tests are triggered automatically with GitHub Actions on every push.

- **Run tests**:
```bash
make test
```