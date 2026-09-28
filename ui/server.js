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
const DATASET_ID = process.env.BQ_DATASET || 'gke_metrics';
const TABLE_ID = process.env.BQ_TABLE || 'training_data';
const RETRAIN_INTERVAL_MS = Number(process.env.RETRAIN_INTERVAL_MS || 30 * 60 * 1000);
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

    // For each service, fetch the committed offsets for its consumer group and compute lag per partition
    let totalLag = 0;
    const perGroup = {};
    for (const service of SERVICE_NAMES) {
      const groupId = `gke-${service}-group`;
      try {
        const groupOffsets = await admin.fetchOffsets({ groupId, topic: KAFKA_TOPIC });
        // groupOffsets: [{ partition: '0', offset: '42', metadata: null }, ...]
        perGroup[groupId] = [];
        for (const p of groupOffsets) {
          const partition = Number(p.partition);
          const committed = Number(p.offset === '-1' ? 0 : p.offset);
          const latest = Number(latestPerPartition[partition] || 0);
          const lag = Math.max(0, latest - committed);
          totalLag += lag;
          perGroup[groupId].push({ partition, committed, latest, lag });
        }
      } catch (e) {
        // If group not found or fetch failed, record the error
        perGroup[groupId] = { error: e.message || String(e) };
      }
    }

    await admin.disconnect();
    return { totalLag, perPartitionLatest: latestPerPartition, perGroup };
  } catch (err) {
    console.warn('computeQueueDepthFromKafka failed:', err.message);
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
  // very short window (3s) for highly responsive throughput reporting
  const windowMs = 3000;
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
    // Do not compute Kafka queue depth (removed - always returned zero). Return real throughput and pilerRate only.
    const messagesPerSecond = SERVICE_NAMES.reduce((sum, s) => sum + computeMessagesPerSecond(s), 0);
    res.json({
      service: 'kafka-message-piler',
      messagesPerSecond,
      pilerRate,
      httpsRequestsServed: currentHttpsRate(),
      computedFromKafka: false
    });
  } catch (err) {
    res.json({
      service: 'kafka-message-piler',
      messagesPerSecond: SERVICE_NAMES.reduce((sum, s) => sum + computeMessagesPerSecond(s), 0),
      pilerRate,
      httpsRequestsServed: currentHttpsRate(),
      computedFromKafka: false
    });
  }
});

// Debug endpoint: return per-partition latest offsets and per-consumer-group committed offsets and lag
app.get('/api/kafka-lag', async (req, res) => {
  try {
    const computed = await computeQueueDepthFromKafka();
    if (computed === null) {
      return res.status(500).json({ ok: false, error: 'Unable to compute Kafka lag (admin API failed or brokers unreachable)' });
    }
    return res.json({ ok: true, data: computed });
  } catch (err) {
    return res.status(500).json({ ok: false, error: err.message });
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

// Set piler to an absolute rate (msg/s)
app.post('/api/piler/set', (req, res) => {
  const rate = Number(req.body?.rate || 0);
  if (!Number.isFinite(rate) || rate <= 0) {
    return res.status(400).json({ ok: false, error: 'invalid rate' });
  }
  pilerRate = Math.max(100, Math.round(rate));
  res.json({ ok: true, rate: pilerRate });
});

// BigQuery helpers: ensure dataset/table exist and stream rows
async function ensureDatasetAndTable() {
  try {
    const dataset = bigquery.dataset(DATASET_ID);
    // create dataset if missing
    const [exists] = await dataset.exists().catch(() => [false]);
    if (!exists) {
      await bigquery.createDataset(DATASET_ID);
      console.log(`Created BigQuery dataset ${DATASET_ID}`);
    }

    const table = dataset.table(TABLE_ID);
    const [tableExists] = await table.exists().catch(() => [false]);
    if (!tableExists) {
      const schema = [
        { name: 'event_time', type: 'TIMESTAMP' },
        { name: 'service', type: 'STRING' },
        { name: 'cpu', type: 'FLOAT' },
        { name: 'memory', type: 'INTEGER' },
        { name: 'cpuUtilization', type: 'FLOAT' },
        { name: 'ramUtilization', type: 'FLOAT' },
        { name: 'messagesPerSecond', type: 'INTEGER' },
        { name: 'requestsPerSecond', type: 'INTEGER' },
        { name: 'httpsRequestsServed', type: 'INTEGER' },
        { name: 'queueDepth', type: 'INTEGER' },
        { name: 'pilerRate', type: 'INTEGER' },
        { name: 'source', type: 'STRING' }
      ];
      await dataset.createTable(TABLE_ID, { schema });
      console.log(`Created BigQuery table ${DATASET_ID}.${TABLE_ID}`);
    }
  } catch (err) {
    console.warn('BigQuery dataset/table ensure failed:', err.message);
  }
}

async function streamMetricsToBigQuery(rows) {
  if (!rows || !rows.length) return;
  try {
    const dataset = bigquery.dataset(DATASET_ID);
    const table = dataset.table(TABLE_ID);
    // Insert expects array of objects
    await table.insert(rows);
    console.log(`Inserted ${rows.length} rows into ${DATASET_ID}.${TABLE_ID}`);
  } catch (err) {
    console.error('BigQuery insert failed:', err.message);
  }
}

async function collectAndStreamMetrics() {
  try {
    await ensureDatasetAndTable();
    const nowTs = new Date().toISOString();
    const kafkaStat = await (async () => { try { const d = await computeQueueDepthFromKafka(); return { queueDepth: d ?? 0, pilerRate }; } catch (e) { return { queueDepth: 0, pilerRate }; } })();
    const rows = [];
    for (const service of SERVICE_NAMES) {
      const metricsFromService = await fetchServiceMetrics(service);
      const mps = metricsFromService ? Number(metricsFromService.messagesPerSecond || computeMessagesPerSecond(service)) : computeMessagesPerSecond(service);
      rows.push({ event_time: nowTs, service, cpu: metricsFromService ? Number(metricsFromService.cpu) : null, memory: metricsFromService ? Number(metricsFromService.memory) : null, cpuUtilization: metricsFromService ? Number(metricsFromService.cpuUtilization) : null, ramUtilization: metricsFromService ? Number(metricsFromService.ramUtilization) : null, messagesPerSecond: mps, requestsPerSecond: metricsFromService ? Number(metricsFromService.requestsPerSecond || 0) : null, httpsRequestsServed: metricsFromService ? Number(metricsFromService.httpsRequestsServed || 0) : null, queueDepth: kafkaStat.queueDepth, pilerRate: kafkaStat.pilerRate, source: 'bff' });
    }
    // aggregate piler row
    rows.push({ event_time: nowTs, service: 'kafka-piler', cpu: null, memory: null, cpuUtilization: null, ramUtilization: null, messagesPerSecond: rows.reduce((s, r) => s + (Number(r.messagesPerSecond || 0)), 0), requestsPerSecond: null, httpsRequestsServed: currentHttpsRate(), queueDepth: kafkaStat.queueDepth, pilerRate: kafkaStat.pilerRate, source: 'bff' });
    await streamMetricsToBigQuery(rows);
  } catch (err) {
    console.error('collectAndStreamMetrics failed:', err.message);
  }
}

async function retrainModels() {
  try {
    console.log('Starting BigQuery ML retrain jobs...');
    const cpuQuery = `CREATE OR REPLACE MODEL \`${DATASET_ID}.model_cpu\` OPTIONS(model_type='linear_reg', input_label_cols=['cpu']) AS SELECT messagesPerSecond AS messages_per_second, service AS microservice_name, cpu AS cpu FROM \`${DATASET_ID}.${TABLE_ID}\` WHERE cpu IS NOT NULL`;
    const memQuery = `CREATE OR REPLACE MODEL \`${DATASET_ID}.model_memory\` OPTIONS(model_type='linear_reg', input_label_cols=['memory']) AS SELECT messagesPerSecond AS messages_per_second, service AS microservice_name, memory AS memory FROM \`${DATASET_ID}.${TABLE_ID}\` WHERE memory IS NOT NULL`;
    await bigquery.createQueryJob({ query: cpuQuery });
    await bigquery.createQueryJob({ query: memQuery });
    console.log('Submitted retrain jobs to BigQuery');
  } catch (err) {
    console.error('Retrain failed:', err.message);
  }
}

// Schedule periodic collection and retrain every RETRAIN_INTERVAL_MS
setInterval(async () => {
  await collectAndStreamMetrics();
  await retrainModels();
}, RETRAIN_INTERVAL_MS);

// Run initial collect and retrain
collectAndStreamMetrics().catch(() => {});
setTimeout(() => retrainModels().catch(() => {}), 5000);

// start consumer after setting up BQ jobs
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