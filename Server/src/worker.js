const { parentPort, workerData } = require('worker_threads');

const processRequest = async (data) => {
    
    await new Promise((resolve) => setTimeout(resolve, 2000));
    parentPort.postMessage({ message: `Processed`, data: data });
};

processRequest(workerData);
