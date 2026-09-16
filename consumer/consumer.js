const { Kafka, logLevel } = require('kafkajs');

const kafka = new Kafka({
  clientId: 'shipping-service',
  brokers: ['localhost:9092'],
  retry: {
    retries: 10   // retry more times if kafka is down, to avoid crashing during demo
  },
  logLevel: logLevel.ERROR
});
const consumerGroup = process.argv[2] || 'shipping'
const consumer = kafka.consumer({ groupId: consumerGroup });

async function run() {
  console.log(`Consumer group: ${consumerGroup}`);
  console.log("Connecting and subscribing to the 'orders' topic...");
  await consumer.connect();
  await consumer.subscribe({ topic: 'orders', fromBeginning: true });

  console.log("Waiting for messages...");
  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const order = JSON.parse(message.value.toString());
      try {
        // add shipping logic
        // throw new Error("Shipping service is down!"); // simulate api crash (if enough time)
        console.log(`[P${partition}] Processing order: ${order.order_id}, user: ${order.user_id}, total: ${order.total}`);
      } catch (err) {
        console.error(`[Erreur offset ${message.offset}]`, err.message);
      }
    }
  });
}

run();