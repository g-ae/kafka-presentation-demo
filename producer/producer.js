const { Kafka } = require('kafkajs');

const kafka = new Kafka({
  clientId: 'checkout-service',
  brokers: ['localhost:9092'],
  retry: {
    retries: 10 // retry more times if kafka is down, to avoid crashing during demo
  }
});

const producer = kafka.producer();

let user_id = 0;

async function run() {
  await producer.connect();

  while (true) {
    // every new order is made by a new user. use incrementation so it is easier to understand
    user_id++;
    
    const order = {
      order_id: `order-${user_id.toString()}-${Date.now().toString()}`,
      user_id: user_id.toString(),
      total: (Math.random() * 200 + 50).toFixed(2)
    };

    await producer.send({
      topic: 'orders',
      messages: [
        {
          key: user_id.toString(),
          value: JSON.stringify(order)
        }
      ]
    });
    console.log(`Order sent to Kafka. Data : order_id: ${order.order_id}, user_id: ${order.user_id}, total: ${order.total}`); 
    await new Promise(resolve => setTimeout(resolve, 3000));
  }
  
}
run();