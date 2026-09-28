const http = require('http');
const { Kafka } = require('kafkajs');

const SERVICE_NAME = process.env.SERVICE_NAME || 'order-processor';
const BROKERS = (process.env.KAFKA_BROKERS || 'kafka-service:9092').split(',').map((s) => s.trim()).filter(Boolean);
const TOPIC = process.env.KAFKA_TOPIC || 'gke-utilization';
const PORT = Number(process.env.PORT || 8081);

const kafka = new Kafka({
  clientId: `kafka-consumer-${SERVICE_NAME}`,
  brokers: BROKERS,
  retry: { initialRetryTime: 300, retries: 10 }
});

const rateWindowMs = 15000;
const recentTimestamps = [];
let lastSnapshot = {
  cpu: 1.0,
  memory: 1024,
  cpuUtilization: 25,
  ramUtilization: 30,
  messagesPerSecond: 0
};

function clamp(value, min, max) {
  return Math.min(Math.max(value, min), max);
}

function computeRate() {
  const now = Date.now();
  while (recentTimestamps.length && now - recentTimestamps[0] > rateWindowMs) {
    recentTimestamps.shift();
  }
  return recentTimestamps.length;
}

function updateSnapshotFromMessage(payload) {
  const nextCpu = Number(payload.cpu_cores_utilized ?? lastSnapshot.cpu ?? 1.0);
  const nextMemory = Number(payload.memory_mib_utilized ?? lastSnapshot.memory ?? 1024);
  const nextCpuUtilization = Number(payload.cpu_utilization ?? lastSnapshot.cpuUtilization ?? ((nextCpu / 8) * 100));
  const nextRamUtilization = Number(payload.ram_utilization ?? lastSnapshot.ramUtilization ?? ((nextMemory / 8192) * 100));
  const nextMessagesPerSecond = computeRate();

  lastSnapshot = {
    cpu: clamp(nextCpu, 0.2, 8),
    memory: clamp(nextMemory, 256, 8192),
    cpuUtilization: clamp(nextCpuUtilization, 0, 100),
    ramUtilization: clamp(nextRamUtilization, 0, 100),
    messagesPerSecond: nextMessagesPerSecond
  };
}

async function startConsumer() {
  const consumer = kafka.consumer({ groupId: `gke-${SERVICE_NAME}-group` });
  await consumer.connect();
  await consumer.subscribe({ topic: TOPIC, fromBeginning: false });

  await consumer.run({
    eachMessage: async ({ message }) => {
      try {
        const payload = JSON.parse(message.value.toString());
        if (payload.service && payload.service !== SERVICE_NAME) {
          return;
        }

        recentTimestamps.push(Date.now());
        updateSnapshotFromMessage(payload);
      } catch (err) {
        console.error(`[${SERVICE_NAME}] failed to parse Kafka event:`, err.message);
      }
    }
  });

  console.log(`[${SERVICE_NAME}] Kafka consumer connected to ${BROKERS.join(', ')} topic=${TOPIC}`);
}

function buildMetrics() {
  const rate = computeRate();
  return {
    service: SERVICE_NAME,
    cpu: Number(lastSnapshot.cpu),
    memory: Number(lastSnapshot.memory),
    cpuUtilization: Number(lastSnapshot.cpuUtilization),
    ramUtilization: Number(lastSnapshot.ramUtilization),
    messagesPerSecond: rate
  };
}

const server = http.createServer((req, res) => {
  if (req.url === '/health') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ ok: true, service: SERVICE_NAME }));
    return;
  }

  if (req.url === '/metrics') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify(buildMetrics()));
    return;
  }

  res.writeHead(404, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify({ error: 'not found' }));
});

server.listen(PORT, async () => {
  console.log(`[${SERVICE_NAME}] metrics endpoint listening on ${PORT}`);
  try {
    await startConsumer();
  } catch (err) {
    console.error(`[${SERVICE_NAME}] Kafka connection failed:`, err.message);
  }
});

process.on('SIGINT', async () => {
  process.exit(0);
});
