const { describe, it, before, after } = require('node:test');
const assert = require('node:assert/strict');
const { execSync } = require('node:child_process');
const { Kafka, logLevel } = require('kafkajs');

const BROKER = process.env.KAFKA_BROKER || 'localhost:9092';
const TOPIC_NAME = process.env.TOPIC_NAME || 'orders';

const kafka = new Kafka({
  clientId: 'failsafe-demo',
  brokers: [BROKER],
  retry: { retries: 10, initialRetryTime: 300 },
  logLevel: logLevel.NOTHING
});

let orderCounter = 0;
function createOrder() {
  orderCounter++;
  return {
    order_id: `order-${orderCounter}-${Date.now()}`,
    user_id: orderCounter.toString(),
    total: (Math.random() * 200 + 50).toFixed(2)
  };
}

async function waitForBroker(timeoutMs = 15000) {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    const admin = kafka.admin();
    try {
      await admin.connect();
      await admin.listTopics();
      await admin.disconnect();
      return true;
    } catch {
      try {
        await admin.disconnect();
      } catch {}
      await new Promise(r => setTimeout(r, 500));
    }
  }
  throw new Error('Kafka broker did not become ready');
}

describe('KAFKA PRESENTATION TESTS (demo)', { timeout: 60000 }, () => {
  let admin;
  let producer;

  before(async () => {
    console.log('-- KAFKA PRESENTATION TESTS (demo)');

    admin = kafka.admin();
    await admin.connect();

    // Reset topic 'orders' with 2 partitions for a clean demo run
    const topics = await admin.listTopics();
    if (topics.includes(TOPIC_NAME)) {
      await admin.deleteTopics({ topics: [TOPIC_NAME] });
      await new Promise(r => setTimeout(r, 1000));
    }
    await admin.createTopics({
      topics: [{ topic: TOPIC_NAME, numPartitions: 2, replicationFactor: 1 }]
    });

    producer = kafka.producer();
    await producer.connect();
    console.log(`Topic '${TOPIC_NAME}' ready with 2 partitions.\n`);
  });

  after(async () => {
    if (producer) {
      try {
        await producer.disconnect();
      } catch {}
    }
    if (admin) {
      try {
        await admin.disconnect();
      } catch {}
    }
    console.log('-- PRESENTATION TESTS COMPLETED SUCCESSFULLY');
  });

  it('1. Poll & Produce Orders (shipping group)', async () => {
    console.log('--- Step 1: Start Consumer (shipping) & Producer');
    const consumer = kafka.consumer({ groupId: 'shipping' });
    await consumer.connect();
    await consumer.subscribe({ topic: TOPIC_NAME, fromBeginning: true });

    const received = [];
    let resolve;
    const allDone = new Promise(res => {
      resolve = res;
    });

    await consumer.run({
      eachMessage: async ({ partition, message }) => {
        const order = JSON.parse(message.value.toString());
        console.log(`[shipping] [P${partition}] Processing order: ${order.order_id}, user: ${order.user_id}, total: ${order.total}`);
        received.push(order);
        if (received.length >= 3) {
          resolve();
        }
      }
    });

    // Wait for consumer group to join
    await new Promise(r => setTimeout(r, 1500));

    // Send 3 orders
    for (let i = 0; i < 3; i++) {
      const order = createOrder();
      await producer.send({
        topic: TOPIC_NAME,
        messages: [{ key: order.user_id, value: JSON.stringify(order) }]
      });
      console.log(`Order sent: order_id: ${order.order_id}, user_id: ${order.user_id}, total: ${order.total}`);
    }

    await allDone;
    assert.equal(received.length, 3);

    await consumer.stop();
    await consumer.disconnect();
  });

  it('2. Simulate Broker Crash & Recovery (make stop / make start)', async () => {
    console.log('\n--- Step 2: Simulate Broker Crash & Recovery ---');

    console.log('Stopping Kafka broker (simulating crash)...');
    try {
      execSync('docker compose stop kafka', { stdio: 'inherit' });
    } catch {
      execSync('docker-compose stop kafka', { stdio: 'inherit' });
    }

    console.log('Broker is down. Waiting 2 seconds...');
    await new Promise(r => setTimeout(r, 2000));

    console.log('Starting Kafka broker again...');
    try {
      execSync('docker compose start kafka', { stdio: 'inherit' });
    } catch {
      execSync('docker-compose up -d kafka', { stdio: 'inherit' });
    }

    await waitForBroker();
    console.log('Broker is back online!');

    // Reconnect producer and consumer post-restart
    await producer.connect();

    const consumer = kafka.consumer({ groupId: 'shipping' });
    await consumer.connect();
    await consumer.subscribe({ topic: TOPIC_NAME, fromBeginning: false });

    let resolve;
    const orderPromise = new Promise(res => {
      resolve = res;
    });
    let receivedOrder = null;

    await consumer.run({
      eachMessage: async ({ partition, message }) => {
        receivedOrder = JSON.parse(message.value.toString());
        console.log(`[shipping post-recovery] [P${partition}] Processing order: ${receivedOrder.order_id}`);
        resolve();
      }
    });

    await new Promise(r => setTimeout(r, 1500));

    const newOrder = createOrder();
    await producer.send({
      topic: TOPIC_NAME,
      messages: [{ key: newOrder.user_id, value: JSON.stringify(newOrder) }]
    });
    console.log(`Order sent post-recovery: ${newOrder.order_id}`);

    await orderPromise;
    assert.ok(receivedOrder);
    assert.equal(receivedOrder.order_id, newOrder.order_id);

    await consumer.stop();
    await consumer.disconnect();
  });

  it('3. Replay All Orders with New Consumer Group (inventory)', async () => {
    console.log('\n--- Step 3: Open another consumer (GROUP_ID=inventory) ---');
    const consumer = kafka.consumer({ groupId: 'inventory' });
    await consumer.connect();
    // fromBeginning: true allows reading all historical events
    await consumer.subscribe({ topic: TOPIC_NAME, fromBeginning: true });

    const received = [];
    let resolve;
    const allDone = new Promise(res => {
      resolve = res;
    });

    await consumer.run({
      eachMessage: async ({ partition, message }) => {
        const order = JSON.parse(message.value.toString());
        console.log(`[inventory] [P${partition}] Replayed order: ${order.order_id}, user: ${order.user_id}`);
        received.push(order);
        if (received.length >= orderCounter) {
          resolve();
        }
      }
    });

    await allDone;
    console.log(`✔ Consumer group "inventory" replayed all ${received.length} orders from the beginning.`);
    assert.equal(received.length, orderCounter);

    await consumer.stop();
    await consumer.disconnect();
  });
});
