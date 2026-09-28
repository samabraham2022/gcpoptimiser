const { Kafka } = require('kafkajs');

const kafkaBrokers = (process.env.KAFKA_BROKERS || 'kafka-service:9092').split(',').map((s) => s.trim()).filter(Boolean);
const topic = process.env.KAFKA_TOPIC || 'gke-utilization';
const services = ['order-processor', 'payment-gateway', 'notification-worker'];

const kafka = new Kafka({
  clientId: 'kafka-message-piler',
  brokers: kafkaBrokers,
  retry: { initialRetryTime: 300, retries: 10 }
});

const producer = kafka.producer();

function makePayload(serviceName, index) {
  const baseCpu = [1.2, 2.8, 0.9][index % 3];
  const baseRam = [1024, 2048, 768][index % 3];
  const baseMps = [420, 780, 210][index % 3];
  const burstFactor = 1.5 + ((Math.sin(Date.now() / 1400 + index) + 1) * 0.8);
  const jitter = Math.sin(Date.now() / 2000 + index) * 0.4;
  const cpu = Math.max(0.2, baseCpu + jitter);
  const memory = Math.max(256, baseRam + (Math.sin(Date.now() / 3000 + index) * 220));
  const messagesPerSecond = Math.max(180, Math.round((baseMps * burstFactor) + Math.abs(Math.cos(Date.now() / 1200 + index)) * 280));
  const cpuUtilization = Math.min(100, Math.max(10, (cpu / 8) * 100));
  const ramUtilization = Math.min(100, Math.max(10, (memory / 8192) * 100));

  return {
    service: serviceName,
    timestamp: new Date().toISOString(),
    cpu_cores_utilized: Number(cpu.toFixed(2)),
    memory_mib_utilized: Math.round(memory),
    cpu_utilization: Number(cpuUtilization.toFixed(1)),
    ram_utilization: Number(ramUtilization.toFixed(1)),
    messages_per_second: messagesPerSecond,
    source: 'kafka-message-piler'
  };
}

async function start() {
  try {
    await producer.connect();
    console.log(`Kafka message piler connected to ${kafkaBrokers.join(', ')}`);

    // Desired total messages per second (across all services). Can be overridden by BFF /api/piler
    let desiredRate = Number(process.env.PILE_RATE || process.env.INIT_PILER_RATE || 100);
    const bffUrl = process.env.BFF_URL || 'http://localhost:3000';

    // Fetch BFF desired rate immediately and poll every 5s
    const fetchBffRate = async () => {
      try {
        const http = require('http');
        return await new Promise((resolve) => {
          http.get(`${bffUrl}/api/piler`, (res) => {
            let data = '';
            res.on('data', (chunk) => data += chunk);
            res.on('end', () => {
              try {
                const obj = JSON.parse(data);
                if (obj?.rate) resolve(Number(obj.rate));
                else resolve(null);
              } catch (e) { resolve(null); }
            });
          }).on('error', () => resolve(null));
        });
      } catch (e) { return null; }
    };

    (async () => {
      const r = await fetchBffRate();
      if (r) desiredRate = r;
    })();

    setInterval(async () => {
      try {
        const r = await fetchBffRate();
        if (r) desiredRate = r;
      } catch (e) {}
    }, 5000);

    // Emit messages at roughly desiredRate per second. Send all messages in one batch to avoid sequential awaits.
    setInterval(async () => {
      try {
        // Distribute desiredRate across services, preserving exact total.
        let base = Math.floor(desiredRate / services.length);
        if (base < 1) base = 1; // ensure at least one per service for safety
        let remainder = desiredRate - (base * services.length);
        if (remainder < 0) remainder = 0;
        const perServiceCounts = services.map(() => (remainder > 0 ? (remainder--, base + 1) : base));
        const allMsgs = [];
        for (let i = 0; i < services.length; i++) {
          const count = perServiceCounts[i];
          for (let n = 0; n < count; n++) {
            const payload = makePayload(services[i], i + n + Math.floor(Math.random() * 1000));
            allMsgs.push({ value: JSON.stringify(payload) });
          }
        }
        if (allMsgs.length) {
          await producer.send({ topic, messages: allMsgs });
          console.log(`Piler sent ${allMsgs.length} messages (desiredRate=${desiredRate}, perService=[${perServiceCounts.join(',')}])`);
        }
      } catch (err) {
        console.error('Failed to publish Kafka telemetry message:', err.message);
      }
    }, 1000);
  } catch (err) {
    console.error('Kafka producer failed to start:', err.message);
    process.exit(1);
  }
}

start();

process.on('SIGINT', async () => {
  await producer.disconnect();
  process.exit(0);
});
