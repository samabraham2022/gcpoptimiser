const path = require('path');
const http = require('http');
const express = require('express');
const k8s = require('@kubernetes/client-node');
const { BigQuery } = require('@google-cloud/bigquery');
const { Kafka } = require('kafkajs');

const app = express();
app.use(express.json());

const bigquery = new BigQuery();
const KAFKA_TOPIC = process.env.KAFKA_TOPIC || 'gke-utilization';
const SERVICE_NAMES = ['order-processor', 'payment-gateway', 'notification-worker'];
const NON_KAFKA_SERVICES = {
  'trade-execution-engine': { cpu: 4.0, memory: 4096, cpuUtilization: 58, ramUtilization: 52, requestsPerSecond: 980, httpsRequestsServed: 960 },
  'batch-processor-pipeline': { cpu: 3.3, memory: 6144, cpuUtilization: 64, ramUtilization: 59, requestsPerSecond: 620, httpsRequestsServed: 610 }
};
const liveMetrics = new Map();
const requestTimestamps = [];
const pilerStatus = {
  messagesPerSecond: 420,
  queueDepth: 1500,
  multiplier: 1
};
const baseMetrics = {
  'order-processor': { cpu: 1.4, memory: 1024, cpuUtilization: 32, ramUtilization: 24, messagesPerSecond: 420, requestsPerSecond: 180, httpsRequestsServed: 150 },
  'payment-gateway': { cpu: 2.6, memory: 2048, cpuUtilization: 48, ramUtilization: 38, messagesPerSecond: 780, requestsPerSecond: 220, httpsRequestsServed: 190 },
  'notification-worker': { cpu: 0.8, memory: 768, cpuUtilization: 22, ramUtilization: 18, messagesPerSecond: 210, requestsPerSecond: 140, httpsRequestsServed: 130 }
};

SERVICE_NAMES.forEach((name) => liveMetrics.set(name, { ...baseMetrics[name] }));

async function fetchServiceMetrics(serviceName) {
  try {
    const url = `http://${serviceName}:8081/metrics`;
    const response = await new Promise((resolve, reject) => {
      const req = http.get(url, (res) => {
        let data = '';
        res.on('data', (chunk) => { data += chunk; });
        res.on('end', () => {
          try {
            resolve(JSON.parse(data));
          } catch (err) {
            reject(err);
          }
        });
      });
      req.on('error', reject);
    });
    return response;
  } catch (err) {
    return null;
  }
}

function refreshSyntheticMetrics() {
  const now = Date.now() / 1000;
  const recentWindowMs = 10 * 1000;
  while (requestTimestamps.length && Date.now() - requestTimestamps[0] > recentWindowMs) {
    requestTimestamps.shift();
  }

  SERVICE_NAMES.forEach((service, index) => {
    const current = liveMetrics.get(service) || { ...baseMetrics[service] };
    const cpuWave = Math.sin(now / 6 + index) * 0.7;
    const memWave = Math.cos(now / 7 + index) * 200;
    const mpsWave = Math.sin(now / 4 + index) * 140;

    const cpu = Math.max(0.2, current.cpu + cpuWave * 0.25);
    const memory = Math.max(256, current.memory + memWave * 0.12);
    const messagesPerSecond = Math.max(60, current.messagesPerSecond + mpsWave * 0.7);
    const requestsPerSecond = Math.max(80, current.requestsPerSecond + Math.sin(now / 5 + index) * 25 + (requestTimestamps.length / 5));
    const httpsRequestsServed = Math.max(70, current.httpsRequestsServed + Math.cos(now / 6 + index) * 18 + (requestTimestamps.length / 6));

    liveMetrics.set(service, {
      cpu: Number(cpu.toFixed(2)),
      memory: Math.round(memory),
      cpuUtilization: Number(Math.min(100, Math.max(10, (cpu / 8) * 100)).toFixed(1)),
      ramUtilization: Number(Math.min(100, Math.max(10, (memory / 8192) * 100)).toFixed(1)),
      messagesPerSecond: Math.round(messagesPerSecond),
      requestsPerSecond: Math.round(requestsPerSecond),
      httpsRequestsServed: Math.round(httpsRequestsServed)
    });
  });

  Object.entries(NON_KAFKA_SERVICES).forEach(([service, metrics], idx) => {
    const current = liveMetrics.get(service) || { ...metrics };
    const requestWave = Math.sin(now / 3 + idx) * 110;
    const httpsWave = Math.cos(now / 4 + idx) * 90;
    const nextReqs = Math.max(300, (metrics.requestsPerSecond || 300) + requestWave + (requestTimestamps.length / 4));
    const nextHttps = Math.max(250, (metrics.httpsRequestsServed || 250) + httpsWave + (requestTimestamps.length / 5));

    liveMetrics.set(service, {
      cpu: Number((metrics.cpu || current.cpu || 2.5).toFixed(2)),
      memory: Number(metrics.memory || current.memory || 2048),
      cpuUtilization: Number((metrics.cpuUtilization || current.cpuUtilization || 45).toFixed(1)),
      ramUtilization: Number((metrics.ramUtilization || current.ramUtilization || 40).toFixed(1)),
      requestsPerSecond: Math.round(nextReqs),
      httpsRequestsServed: Math.round(nextHttps)
    });
  });
}

async function startKafkaConsumer() {
  const brokers = (process.env.KAFKA_BROKERS || 'kafka-service:9092').split(',').map((s) => s.trim()).filter(Boolean);
  if (!brokers.length) return;

  try {
    const kafka = new Kafka({
      clientId: 'gke-ui-metrics-consumer',
      brokers,
      retry: { initialRetryTime: 300, retries: 10 }
    });
    const consumer = kafka.consumer({ groupId: 'gke-ui-metrics-group' });
    await consumer.connect();
    await consumer.subscribe({ topic: KAFKA_TOPIC, fromBeginning: false });
    await consumer.run({
      eachMessage: async ({ message }) => {
        try {
          const payload = JSON.parse(message.value.toString());
          const service = payload.service || 'order-processor';
          if (!liveMetrics.has(service)) {
            liveMetrics.set(service, { ...baseMetrics['order-processor'] });
          }
          liveMetrics.set(service, {
            cpu: Number(payload.cpu_cores_utilized ?? liveMetrics.get(service).cpu),
            memory: Number(payload.memory_mib_utilized ?? liveMetrics.get(service).memory),
            cpuUtilization: Number(payload.cpu_utilization ?? liveMetrics.get(service).cpuUtilization),
            ramUtilization: Number(payload.ram_utilization ?? liveMetrics.get(service).ramUtilization),
            messagesPerSecond: Number(payload.messages_per_second ?? liveMetrics.get(service).messagesPerSecond)
          });
        } catch (err) {
          console.error('Unable to parse Kafka telemetry message:', err.message);
        }
      }
    });
    console.log(`Kafka consumer connected for topic ${KAFKA_TOPIC}`);
  } catch (err) {
    console.warn('Kafka consumer unavailable; using synthetic live metrics fallback.', err.message);
  }
}

function currentHttpsRate() {
  const windowMs = 15000;
  while (requestTimestamps.length && Date.now() - requestTimestamps[0] > windowMs) {
    requestTimestamps.shift();
  }
  return requestTimestamps.length;
}

function getKafkaPilerMetrics() {
  const adjustedRate = Math.max(100, Math.round((pilerStatus.messagesPerSecond || 420) * (pilerStatus.multiplier || 1)));
  const queueDelta = Math.max(0, Math.round((pilerStatus.queueDepth || 1500) * (pilerStatus.multiplier || 1) / 6));
  return {
    messagesPerSecond: adjustedRate,
    queueDepth: Math.max(120, Math.min(50000, (pilerStatus.queueDepth || 1500) + queueDelta)),
    multiplier: Number(pilerStatus.multiplier || 1)
  };
}

app.use((req, res, next) => {
  if (req.path !== '/health' && !req.path.startsWith('/static') && !req.path.startsWith('/favicon')) {
    requestTimestamps.push(Date.now());
  }
  next();
});

app.get('/api/kafka-status', async (req, res) => {
  const fallback = getKafkaPilerMetrics();
  res.json({
    service: 'kafka-message-piler',
    messagesPerSecond: fallback.messagesPerSecond,
    queueDepth: fallback.queueDepth,
    multiplier: fallback.multiplier,
    httpsRequestsServed: currentHttpsRate()
  });
});

app.post('/api/increase-piler-rate', (req, res) => {
  const nextFactor = Number(req.body?.factor || 1.5);
  pilerStatus.multiplier = Number((pilerStatus.multiplier || 1) * nextFactor).toFixed(2);
  pilerStatus.messagesPerSecond = Math.max(500, Math.round((pilerStatus.messagesPerSecond || 420) * nextFactor));
  pilerStatus.queueDepth = Math.max(1500, Math.round((pilerStatus.queueDepth || 1500) * nextFactor));

  res.json({
    ok: true,
    messagesPerSecond: pilerStatus.messagesPerSecond,
    queueDepth: pilerStatus.queueDepth,
    multiplier: Number(pilerStatus.multiplier)
  });
});

setInterval(refreshSyntheticMetrics, 4000);
startKafkaConsumer();

// Initialize Kubernetes in-cluster client with fallback
let k8sApi = null;
try {
  const kc = new k8s.KubeConfig();
  kc.loadFromCluster();
  k8sApi = kc.makeApiClient(k8s.CoreV1Api);
} catch (e) {
  console.warn('Kubernetes in-cluster config not found. Running with mock pods fallback.');
}

app.get('/api/live-metrics', async (req, res) => {
  const serviceMetrics = [];
  for (const service of SERVICE_NAMES) {
    const metricsFromService = await fetchServiceMetrics(service);
    if (metricsFromService) {
      serviceMetrics.push({
        id: service,
        name: service.replace(/-/g, ' ').replace(/\b\w/g, (c) => c.toUpperCase()),
        cpu: Number(metricsFromService.cpu),
        memory: Number(metricsFromService.memory),
        cpuUtilization: Number(metricsFromService.cpuUtilization),
        ramUtilization: Number(metricsFromService.ramUtilization),
        messagesPerSecond: Number(metricsFromService.messagesPerSecond || 0),
        requestsPerSecond: Number(metricsFromService.requestsPerSecond || 0),
        httpsRequestsServed: Number(metricsFromService.httpsRequestsServed || 0)
      });
    } else {
      const metrics = liveMetrics.get(service) || { ...baseMetrics[service] };
      serviceMetrics.push({
        id: service,
        name: service.replace(/-/g, ' ').replace(/\b\w/g, (c) => c.toUpperCase()),
        cpu: Number(metrics.cpu),
        memory: Number(metrics.memory),
        cpuUtilization: Number(metrics.cpuUtilization),
        ramUtilization: Number(metrics.ramUtilization),
        messagesPerSecond: Number(metrics.messagesPerSecond || 0),
        requestsPerSecond: Number(metrics.requestsPerSecond || 0),
        httpsRequestsServed: Number(metrics.httpsRequestsServed || 0)
      });
    }
  }
  res.json({ services: serviceMetrics });
});

app.get('/api/https-requests', (req, res) => {
  res.json({
    requestsPerSecond: currentHttpsRate(),
    totalSeen: requestTimestamps.length,
    label: 'HTTPS requests served'
  });
});

// 1. Fetch running services/pods from GKE
app.get('/api/services', async (req, res) => {
  if (!k8sApi) {
    return res.json({
      services: ['order-processor-79bf4', 'payment-gateway-9bc21', 'notification-worker-34df2']
    });
  }

  try {
    const response = await k8sApi.listNamespacedPod('default');
    const runningPods = response.body.items
      .filter((pod) => pod.status.phase === 'Running')
      .map((pod) => pod.metadata.name);
    res.json({ services: runningPods.length > 0 ? runningPods : ['order-processor-demo'] });
  } catch (err) {
    console.error('K8s API query failed:', err.message);
    res.json({
      services: ['order-processor-fallback', 'payment-gateway-fallback']
    });
  }
});

// 2. Query BigQuery ML models for CPU and Memory predictions
app.post('/api/predict', async (req, res) => {
  try {
    const { messages_per_second, microservice_name } = req.body.features || {};
    const mps = parseInt(messages_per_second, 10) || 500;
    const sName = (microservice_name || 'order-processor').replace(/[^a-zA-Z0-9_-]/g, '');
    const datasetId = 'gke_metrics';

    const query = `
      SELECT 
        (SELECT predicted_cpu_cores_utilized FROM ML.PREDICT(MODEL \`${datasetId}.model_cpu\`, 
          (SELECT ${mps} AS messages_per_second, '${sName}' AS microservice_name))) AS cpu,
        (SELECT predicted_memory_mib_utilized FROM ML.PREDICT(MODEL \`${datasetId}.model_memory\`, 
          (SELECT ${mps} AS messages_per_second, '${sName}' AS microservice_name))) AS memory
    `;

    const [job] = await bigquery.createQueryJob({ query });
    const [rows] = await job.getQueryResults();

    const cpuVal = rows[0]?.cpu ? Math.max(0.1, rows[0].cpu) : 1.2;
    const memVal = rows[0]?.memory ? Math.max(256, rows[0].memory) : 1024;

    res.json({
      predictions: [
        {
          cpu_cores_utilized: cpuVal,
          memory_mib_utilized: memVal
        }
      ]
    });
  } catch (err) {
    console.error('BigQuery prediction error:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// Serve compiled React build
app.use(express.static(path.join(__dirname, 'build')));
app.get('*', (req, res) => {
  res.sendFile(path.join(__dirname, 'build', 'index.html'));
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`BFF listening on port ${PORT}`));