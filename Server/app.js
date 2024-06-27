const express = require('express');
const NodeCache = require('node-cache');
const { Worker } = require('worker_threads');
const { publishToQueue, consumeFromQueue } = require('./src/rabbitmq');

const app = express();
const port = 3000;
const cache = new NodeCache();

app.use(express.json());

const runWorker = (data) => {
    return new Promise((resolve, reject) => {
        const worker = new Worker('./src/worker.js', {
            workerData: data,
        });
        worker.on('message', resolve);
        worker.on('error', reject);
        worker.on('exit', (code) => {
            if (code !== 0) {
                reject(new Error(`Worker stopped with exit code ${code}`));
            }
        });
    });
};

app.post('/api/apply', async (req, res) => {
    try {
        validateInput(req.body);
        const { reqId, resource, action, clusterId } = req.body;

        // Create a promise and store it in the cache
        const responsePromise = new Promise((resolve, reject) => {
            cache.set(reqId, { resolve, reject });
        });

        const result = await runWorker({ resource, action, reqId, clusterId });
        const queueName = `Cluster-${clusterId}-Queue`;

        await publishToQueue(JSON.stringify(result), queueName);

        // Wait for the response to be received
        const response = await responsePromise;
        res.status(200).json(response);
    } catch (error) {
        console.error(`Validation or processing error for request ${req.body.reqId}:`, error);
        res.status(400).json({ error: error.message });
    }
});

function validateInput(data) {
    const { resource, action, reqId, clusterId } = data;
    if (typeof resource !== 'string' || typeof action !== 'string' || typeof reqId !== 'string' || typeof clusterId !== 'number') {
        throw new Error("Validation failed: Incorrect input types");
    }
}

const startResponseListener = () => {
    consumeFromQueue(process.env.RESPONSE_QUEUE_NAME, (message) => {
        const { reqId, result } = JSON.parse(message.content.toString());
        const requestData = cache.get(reqId);
        if (requestData) {
            requestData.resolve(result);
            cache.del(reqId);
        } else {
            console.error(`No cache entry found for reqId ${reqId}`);
        }
    });
};

app.listen(port, () => {
    console.log(`Server running at http://localhost:${port}/`);
    startResponseListener();
});
