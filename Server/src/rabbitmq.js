const amqp = require('amqplib');
require('dotenv').config();

const publishToQueue = async (message, queueName) => {
    try {
        const connection = await amqp.connect(process.env.RABBITMQ_URL);
        const channel = await connection.createChannel();
        await channel.assertQueue(queueName, { durable: true });
        channel.sendToQueue(queueName, Buffer.from(message), { persistent: true });
        console.log(`[x] Sent to ${queueName}: '${message}'`);
        await channel.close();
        await connection.close();
    } catch (error) {
        console.error(`Error publishing to queue ${queueName}:`, error);
        throw error;  
    }
};

const consumeFromQueue = async (queueName, onMessage) => {
    try {
        const connection = await amqp.connect(process.env.RABBITMQ_URL);
        const channel = await connection.createChannel();

        console.log(`[x] Waiting for messages in ${queueName}. To exit press CTRL+C`);

        channel.consume(queueName, (msg) => {
            if (msg !== null) {
                console.log(`[x] Received: ${msg.content.toString()}`+ "\n");
                onMessage(msg);  // Processing the message with the callback function
                channel.ack(msg);
            }
        }, { noAck: false });

        process.on('exit', () => {
            channel.close();
            console.log('Closing rabbitMQ channel');
        });

    } catch (error) {
        console.error(`Error subscribing to queue ${queueName}:`, error);
        throw error;
    }
};

module.exports = { publishToQueue, consumeFromQueue };
