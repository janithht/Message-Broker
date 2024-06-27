const amqp = require('amqplib');
require('dotenv').config();

const consumeMessages = async () => {
    try {
        const connection = await amqp.connect(process.env.RABBITMQ_URL);
        const channel = await connection.createChannel();

        channel.on('error', (err) => {
            console.error('Channel error:', err.message);
            if (err.code === 404) {
                console.log(`Queue ${process.env.QUEUE_NAME} does not exist. Waiting to try again...`);
                setTimeout(() => consumeMessages(), 5000); 
            } else {
                console.error('Unexpected channel error:', err);
                process.exit(1); 
            }
        });

        channel.on('close', () => {
            console.log('Channel closed, attempting to reconnect...');
            setTimeout(() => consumeMessages(), 5000);
        });

        console.log(`Waiting for messages in ${process.env.QUEUE_NAME}. To exit press CTRL+C`);
        
        channel.consume(process.env.QUEUE_NAME, async (msg) => {
            if (msg) {
                const messageContent = msg.content.toString();
                console.log(`[x] Received: ${messageContent}`);

                const requestData = JSON.parse(messageContent);
                const { reqId, resource, action, clusterId } = requestData.data;

                const result = await processTask({ resource, action, clusterId });
                
                const responseQueue = process.env.RESPONSE_QUEUE_NAME;
                const response = JSON.stringify({ reqId, result, status: 'success'});
                channel.sendToQueue(responseQueue, Buffer.from(response), { persistent: true });
                console.log(`[x] Sent to ${responseQueue}: '${response}'`);
                channel.ack(msg);
            }
        }, { noAck: false });

    } catch (error) {
        console.error('Failed to connect or set up channel:', error);
        setTimeout(() => consumeMessages(), 5000); 
    }
};

const processTask = async ({ resource, action, clusterId }) => {
    return `Processed action '${action}' on resource '${resource}' for cluster ID ${clusterId}`;
};

consumeMessages();
