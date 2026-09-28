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
// No synthetic defaults: metrics come from service endpoints and Kafka offsets
const liveMetrics = new Map();
const requestTimestamps = [];

// Track recent Kafka message timestamps per service to compute real throughput
const recentTimestampsByService = new Map();
// Piler control: desired messages per second (total across services)
let pilerRate = Number(process.env.INIT_PILER_RATE || 100);

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
  // No-op: removed synthetic metric refresh. Metrics are updated from service endpoints and Kafka consumer.
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
              liveMetrics.set(service, {});
          }
          liveMetrics.set(service, {
            cpu: Number(payload.cpu_cores_utilized ?? liveMetrics.get(service).cpu),
            memory: Number(payload.memory_mib_utilized ?? liveMetrics.get(service).memory),
            cpuUtilization: Number(payload.cpu_utilization ?? liveMetrics.get(service).cpuUtilization),
            ramUtilization: Number(payload.ram_utilization ?? liveMetrics.get(service).ramUtilization),
              // messagesPerSecond will be computed from recent timestamps
          });
          // Track timestamps for throughput calculation
          try {
            const arr = recentTimestampsByService.get(service) || [];
            arr.push(Date.now());
            recentTimestampsByService.set(service, arr);
          } catch (e) {
            // ignore
          }
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

// Compute queue depth using Kafka offsets for each service consumer group.
async function computeQueueDepthFromKafka() {
  const brokers = (process.env.KAFKA_BROKERS || 'kafka-service:9092').split(',').map((s) => s.trim()).filter(Boolean);
  if (!brokers.length) return null;

  try {
    const kafka = new Kafka({ clientId: 'gke-queue-inspector', brokers, retry: { initialRetryTime: 300, retries: 2 } });
    const admin = kafka.admin();
    await admin.connect();

    // Get latest offsets for topic partitions
    const topicOffsets = await admin.fetchTopicOffsets(KAFKA_TOPIC);
    // topicOffsets: [{ partition: '0', offset: '123' }, ...]
    const latestPerPartition = topicOffsets.reduce((acc, p) => {
      acc[Number(p.partition)] = Number(p.offset);
      return acc;
    }, {});

    // For each service, fetch the committed offsets for its consumer group and sum the lag
    let totalLag = 0;
    for (const service of SERVICE_NAMES) {
      const groupId = `gke-${service}-group`;
      try {
        const groupOffsets = await admin.fetchOffsets({ groupId, topic: KAFKA_TOPIC });
        // groupOffsets: [{ partition: '0', offset: '42', metadata: null }, ...]
        for (const p of groupOffsets) {
          const partition = Number(p.partition);
          const committed = Number(p.offset === '-1' ? 0 : p.offset);
          const latest = Number(latestPerPartition[partition] || 0);
          const lag = Math.max(0, latest - committed);
          totalLag += lag;
        }
      } catch (e) {
        // If group not found or fetch failed, ignore this group
        continue;
      }
    }

    await admin.disconnect();
    return totalLag;
  } catch (err) {
    return null;
  }
}

function currentHttpsRate() {
  const windowMs = 15000;
  while (requestTimestamps.length && Date.now() - requestTimestamps[0] > windowMs) {
    requestTimestamps.shift();
  }
  return requestTimestamps.length;
}

function computeMessagesPerSecond(service) {
  const windowMs = 15000;
  const now = Date.now();
  const arr = recentTimestampsByService.get(service) || [];
  while (arr.length && now - arr[0] > windowMs) {
    arr.shift();
  }
  recentTimestampsByService.set(service, arr);
  // rate in messages per second
  return Math.round((arr.length / windowMs) * 1000);
}

// Removed synthetic piler metrics; queue depth is derived from Kafka offsets.

app.use((req, res, next) => {
  if (req.path !== '/health' && !req.path.startsWith('/static') && !req.path.startsWith('/favicon')) {
    requestTimestamps.push(Date.now());
  }
  next();
});

app.get('/api/kafka-status', async (req, res) => {
  try {
    const computedDepth = await computeQueueDepthFromKafka();
    // Sum messages/sec across services (computed from Kafka timestamps)
    const messagesPerSecond = SERVICE_NAMES.reduce((sum, s) => sum + computeMessagesPerSecond(s), 0);
    res.json({
      service: 'kafka-message-piler',
      messagesPerSecond,
      queueDepth: computedDepth !== null ? computedDepth : 0,
      pilerRate,
      httpsRequestsServed: currentHttpsRate(),
      computedFromKafka: computedDepth !== null
    });
  } catch (err) {
    res.json({
      service: 'kafka-message-piler',
      messagesPerSecond: SERVICE_NAMES.reduce((sum, s) => sum + computeMessagesPerSecond(s), 0),
      queueDepth: 0,
      pilerRate,
      httpsRequestsServed: currentHttpsRate(),
      computedFromKafka: false
    });
  }
});

// Piler control endpoints
app.get('/api/piler', (req, res) => {
  res.json({ rate: pilerRate });
});

app.post('/api/piler/change', (req, res) => {
  const delta = Number(req.body?.delta || 0);
  // enforce minimum 100
  pilerRate = Math.max(100, Math.round((pilerRate || 100) + delta));
  res.json({ ok: true, rate: pilerRate });
});

// Removed synthetic piler control endpoints and synthetic metric refresh interval.
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
        messagesPerSecond: Number(metricsFromService.messagesPerSecond || computeMessagesPerSecond(service)),
        requestsPerSecond: Number(metricsFromService.requestsPerSecond || 0),
        httpsRequestsServed: Number(metricsFromService.httpsRequestsServed || 0),
        available: true
      });
    } else {
      const metrics = liveMetrics.get(service) || {};
      serviceMetrics.push({
        id: service,
        name: service.replace(/-/g, ' ').replace(/\b\w/g, (c) => c.toUpperCase()),
        cpu: metrics.cpu ?? null,
        memory: metrics.memory ?? null,
        cpuUtilization: metrics.cpuUtilization ?? null,
        ramUtilization: metrics.ramUtilization ?? null,
        messagesPerSecond: computeMessagesPerSecond(service),
        requestsPerSecond: metrics.requestsPerSecond ?? null,
        httpsRequestsServed: metrics.httpsRequestsServed ?? null,
        available: false
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