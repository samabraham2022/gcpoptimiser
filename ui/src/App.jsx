import React, { useState, useEffect, useCallback } from 'react';
import {
  Server,
  Activity,
  Cpu,
  Database,
  Box,
  Zap,
  CheckCircle2,
  AlertTriangle,
  Flame,
  Copy,
  Terminal,
  Cloud,
  ArrowLeft,
  Layers,
  TrendingUp,
  BarChart3,
  Network,
  RefreshCw
} from 'lucide-react';

const BASE_CATALOG = [
  {
    id: 'order-processor',
    name: 'Order Processor',
    description: 'Handles transactional e-commerce checkouts.',
    iconType: 'box',
    kafkaConsumer: true,
    current: { cpu: 1.5, mem: 1024, mps: 300, requestsPerSecond: 180 }
  },
  {
    id: 'payment-gateway',
    name: 'Payment Gateway',
    description: 'Encrypted payment validation and routing.',
    iconType: 'zap',
    kafkaConsumer: true,
    current: { cpu: 2.0, mem: 2048, mps: 450, requestsPerSecond: 220 }
  },
  {
    id: 'notification-worker',
    name: 'Notification Worker',
    description: 'Asynchronous email and SMS dispatch.',
    iconType: 'activity',
    kafkaConsumer: true,
    current: { cpu: 0.5, mem: 512, mps: 150, requestsPerSecond: 140 }
  },
  {
    id: 'trade-execution-engine',
    name: 'Trade Execution Engine',
    description: 'Low-latency financial order matching.',
    iconType: 'trending',
    kafkaConsumer: false,
    current: { cpu: 4.0, mem: 4096, mps: 1200, requestsPerSecond: 980 }
  },
  {
    id: 'batch-processor-pipeline',
    name: 'Batch Processor Pipeline',
    description: 'Nightly ETL and data synchronization.',
    iconType: 'layers',
    kafkaConsumer: false,
    current: { cpu: 3.5, mem: 6144, mps: 800, requestsPerSecond: 620 }
  }
];

const MAX_CPU = 8.0;
const MAX_MEM = 8192;

const renderIcon = (type) => {
  switch (type) {
    case 'zap': return <Zap className="w-5 h-5 text-amber-400" />;
    case 'activity': return <Activity className="w-5 h-5 text-emerald-400" />;
    case 'trending': return <TrendingUp className="w-5 h-5 text-rose-400" />;
    case 'layers': return <Layers className="w-5 h-5 text-purple-400" />;
    default: return <Box className="w-5 h-5 text-blue-400" />;
  }
};

const getStatusColor = (percentage) => {
  if (percentage < 60) return 'bg-emerald-500 shadow-emerald-500/50';
  if (percentage < 85) return 'bg-amber-500 shadow-amber-500/50';
  return 'bg-rose-500 shadow-rose-500/50';
};

const getStatusText = (percentage) => {
  if (percentage < 60) return { text: 'Optimal', icon: <CheckCircle2 className="w-4 h-4 text-emerald-500" />, color: 'text-emerald-500' };
  if (percentage < 85) return { text: 'Warning', icon: <AlertTriangle className="w-4 h-4 text-amber-500" />, color: 'text-amber-500' };
  return { text: 'Critical', icon: <Flame className="w-4 h-4 text-rose-500" />, color: 'text-rose-500' };
};

export default function App() {
  const [view, setView] = useState('home');
  const [services, setServices] = useState(BASE_CATALOG);
  const [activeService, setActiveService] = useState(null);
  const [mps, setMps] = useState(500);
  const [isPredicting, setIsPredicting] = useState(false);
  const [isLoadingCluster, setIsLoadingCluster] = useState(false);
  const [prediction, setPrediction] = useState(null);
  const [copied, setCopied] = useState(false);
  const [errorMsg, setErrorMsg] = useState(null);
  const [kafkaStatus, setKafkaStatus] = useState({ messagesPerSecond: 0, queueDepth: 0, multiplier: 1 });
  const [httpsRate, setHttpsRate] = useState(0);
  const [isIncreasingPiler, setIsIncreasingPiler] = useState(false);

  const fetchLiveServices = useCallback(async () => {
    setIsLoadingCluster(true);
    setErrorMsg(null);
    try {
      const [servicesRes, metricsRes] = await Promise.all([
        fetch('/api/services'),
        fetch('/api/live-metrics')
      ]);

      if (!servicesRes.ok) throw new Error(`Services HTTP ${servicesRes.status}`);
      if (!metricsRes.ok) throw new Error(`Metrics HTTP ${metricsRes.status}`);

      const servicesData = await servicesRes.json();
      const metricsData = await metricsRes.json();
      const liveMetricsMap = new Map((metricsData.services || []).map((item) => [item.id, item]));

      if (Array.isArray(servicesData.services) && servicesData.services.length > 0) {
        const merged = servicesData.services.map((podName, idx) => {
          const matched = BASE_CATALOG.find((s) => podName.startsWith(s.id));
          const baseService = matched || {
            id: podName,
            name: podName.replace(/-/g, ' ').replace(/\b\w/g, (c) => c.toUpperCase()),
            description: 'Discovered workload running inside default namespace.',
            iconType: ['box', 'zap', 'activity', 'layers'][idx % 4],
            current: { cpu: 1.0, mem: 1024, mps: 250 }
          };

          const liveMetrics = liveMetricsMap.get(baseService.id) || null;
          const current = {
            cpu: liveMetrics ? Number(liveMetrics.cpu || baseService.current.cpu) : baseService.current.cpu,
            mem: liveMetrics ? Number(liveMetrics.memory || baseService.current.mem) : baseService.current.mem,
            mps: liveMetrics ? Number(liveMetrics.messagesPerSecond || baseService.current.mps) : baseService.current.mps,
            requestsPerSecond: liveMetrics ? Number(liveMetrics.requestsPerSecond || baseService.current.requestsPerSecond || 0) : (baseService.current.requestsPerSecond || 0),
            httpsRequestsServed: liveMetrics ? Number(liveMetrics.httpsRequestsServed || baseService.current.httpsRequestsServed || 0) : (baseService.current.httpsRequestsServed || 0),
            cpuUtilization: liveMetrics ? Number(liveMetrics.cpuUtilization || ((Number(liveMetrics.cpu || baseService.current.cpu) / MAX_CPU) * 100)) : ((baseService.current.cpu / MAX_CPU) * 100),
            ramUtilization: liveMetrics ? Number(liveMetrics.ramUtilization || ((Number(liveMetrics.memory || baseService.current.mem) / MAX_MEM) * 100)) : ((baseService.current.mem / MAX_MEM) * 100)
          };

          return { ...baseService, current, livePodName: podName };
        });
        setServices(merged);
      } else {
        const fallback = BASE_CATALOG.map((service) => {
          const liveMetrics = liveMetricsMap.get(service.id);
          return {
            ...service,
            current: {
              cpu: liveMetrics ? Number(liveMetrics.cpu || service.current.cpu) : service.current.cpu,
              mem: liveMetrics ? Number(liveMetrics.memory || service.current.mem) : service.current.mem,
              mps: liveMetrics ? Number(liveMetrics.messagesPerSecond || service.current.mps) : service.current.mps,
              requestsPerSecond: liveMetrics ? Number(liveMetrics.requestsPerSecond || service.current.requestsPerSecond || 0) : (service.current.requestsPerSecond || 0),
              httpsRequestsServed: liveMetrics ? Number(liveMetrics.httpsRequestsServed || service.current.httpsRequestsServed || 0) : (service.current.httpsRequestsServed || 0),
              cpuUtilization: liveMetrics ? Number(liveMetrics.cpuUtilization || ((Number(liveMetrics.cpu || service.current.cpu) / MAX_CPU) * 100)) : ((service.current.cpu / MAX_CPU) * 100),
              ramUtilization: liveMetrics ? Number(liveMetrics.ramUtilization || ((Number(liveMetrics.memory || service.current.mem) / MAX_MEM) * 100)) : ((service.current.mem / MAX_MEM) * 100)
            }
          };
        });
        setServices(fallback);
      }
    } catch (err) {
      console.warn('Unable to query live pods. Retaining catalog.', err.message);
    } finally {
      setIsLoadingCluster(false);
    }
  }, []);

  const fetchKafkaStatus = useCallback(async () => {
    try {
      const res = await fetch('/api/kafka-status');
      if (!res.ok) return;
      const data = await res.json();
      setKafkaStatus({
        messagesPerSecond: Number(data.messagesPerSecond || 0),
        queueDepth: Number(data.queueDepth || 0),
        multiplier: Number(data.multiplier || 1)
      });
    } catch (err) {
      console.warn('Kafka status unavailable:', err.message);
    }
  }, []);

  const fetchHttpsRate = useCallback(async () => {
    try {
      const res = await fetch('/api/https-requests');
      if (!res.ok) return;
      const data = await res.json();
      setHttpsRate(Number(data.requestsPerSecond || 0));
    } catch (err) {
      console.warn('HTTPS rate unavailable:', err.message);
    }
  }, []);

  useEffect(() => {
    fetchLiveServices();
    fetchKafkaStatus();
    fetchHttpsRate();
    const interval = setInterval(() => {
      fetchLiveServices();
      fetchKafkaStatus();
      fetchHttpsRate();
    }, 5000);
    return () => clearInterval(interval);
  }, [fetchLiveServices, fetchKafkaStatus, fetchHttpsRate]);

  useEffect(() => {
    if (activeService) {
      setMps(activeService.current.mps);
      setPrediction(null);
      setErrorMsg(null);
    }
  }, [activeService]);

  const runPrediction = async () => {
    setIsPredicting(true);
    setErrorMsg(null);

    try {
      const response = await fetch('/api/predict', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          features: {
            microservice_name: activeService.id,
            messages_per_second: mps
          }
        })
      });

      if (!response.ok) throw new Error(`Server returned status ${response.status}`);
      const result = await response.json();

      const predictedCpu = parseFloat(result.predictions[0].cpu_cores_utilized);
      const predictedMem = parseFloat(result.predictions[0].memory_mib_utilized);

      const boundedCpu = Math.min(MAX_CPU, Math.max(0.1, predictedCpu));
      const boundedMem = Math.min(MAX_MEM, Math.max(256, predictedMem));

      setPrediction({
        cpu: boundedCpu.toFixed(2),
        mem: Math.round(boundedMem),
        cpuPercent: (boundedCpu / MAX_CPU) * 100,
        memPercent: (boundedMem / MAX_MEM) * 100
      });
    } catch (err) {
      console.warn('API error, falling back to local model approximation:', err.message);
      const baseCpu = 0.15;
      const linearCpu = mps * 0.0018;
      const penalty = mps > 1400 ? Math.pow(mps - 1400, 1.3) * 0.0008 : 0;
      const calcCpu = Math.min(MAX_CPU, Math.max(0.1, baseCpu + linearCpu + penalty));

      const baseMem = 384;
      const linearMem = mps * 0.65;
      const calcMem = Math.min(MAX_MEM, Math.max(256, baseMem + linearMem));

      setPrediction({
        cpu: calcCpu.toFixed(2),
        mem: Math.round(calcMem),
        cpuPercent: (calcCpu / MAX_CPU) * 100,
        memPercent: (calcMem / MAX_MEM) * 100
      });
    } finally {
      setIsPredicting(false);
    }
  };

  const increasePilerRate = async () => {
    setIsIncreasingPiler(true);
    try {
      await fetch('/api/increase-piler-rate', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ factor: 1.5 })
      });
      await fetchKafkaStatus();
    } catch (err) {
      console.warn('Unable to increase piler rate:', err.message);
    } finally {
      setIsIncreasingPiler(false);
    }
  };

  const handleCopyYAML = () => {
    if (!prediction || !activeService) return;

    const yamlText = `apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: ${activeService.id}-hpa
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: ${activeService.id}
  minReplicas: 1
  maxReplicas: 5
  metrics:
  - type: Resource
    resource:
      name: cpu
      target:
        type: Utilization
        averageUtilization: 75
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: ${activeService.id}
spec:
  template:
    spec:
      containers:
      - name: ${activeService.id}-app
        resources:
          requests:
            cpu: "${prediction.cpu}"
            memory: "${prediction.mem}Mi"
          limits:
            cpu: "${Math.min(MAX_CPU, (prediction.cpu * 1.5)).toFixed(2)}"
            memory: "${Math.min(MAX_MEM, (prediction.mem * 1.25)).toFixed(0)}Mi"`;

    navigator.clipboard.writeText(yamlText);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };

  const renderHome = () => (
    <div className="space-y-6">
      <div className="flex items-center justify-between mb-8">
        <div>
          <h2 className="text-xl font-bold text-white flex items-center gap-2">
            <Network className="w-5 h-5 text-blue-400" />
            Active Cluster Workloads
          </h2>
          <p className="text-sm text-slate-400 mt-1">Select a microservice to simulate traffic and optimize Kubernetes resources.</p>
        </div>
        <div className="flex items-center gap-3">
          <button
            onClick={fetchLiveServices}
            disabled={isLoadingCluster}
            className="flex items-center gap-1.5 px-3 py-2 bg-slate-800/80 hover:bg-slate-700 rounded-lg border border-slate-700 text-xs font-mono text-slate-300 transition-colors"
          >
            <RefreshCw className={`w-3.5 h-3.5 ${isLoadingCluster ? 'animate-spin' : ''}`} />
            Sync Cluster
          </button>
          <div className="px-4 py-2 bg-slate-800/50 rounded-lg border border-slate-700/50 text-sm font-mono text-slate-300">
            us-east1-gke-prod
          </div>
        </div>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mb-8">
        <div className="bg-slate-900/50 border border-slate-700/60 rounded-2xl p-4">
          <div className="text-xs uppercase tracking-wider text-slate-400">Kafka Message Piler</div>
          <div className="mt-2 text-2xl font-bold text-white">{Math.round(kafkaStatus.messagesPerSecond)} <span className="text-sm text-slate-400">msg/s</span></div>
          <div className="mt-1 text-xs text-slate-400">Messages being piled per second</div>
        </div>
        <div className="bg-slate-900/50 border border-slate-700/60 rounded-2xl p-4">
          <div className="text-xs uppercase tracking-wider text-slate-400">Kafka Queue Depth</div>
          <div className="mt-2 text-2xl font-bold text-white">{Math.round(kafkaStatus.queueDepth)} <span className="text-sm text-slate-400">queued</span></div>
          <div className="mt-1 text-xs text-slate-400">Messages currently in queue</div>
        </div>
        <div className="bg-slate-900/50 border border-slate-700/60 rounded-2xl p-4">
          <div className="text-xs uppercase tracking-wider text-slate-400">UI HTTPS Served</div>
          <div className="mt-2 text-2xl font-bold text-white">{Math.round(httpsRate)} <span className="text-sm text-slate-400">req/s</span></div>
          <div className="mt-1 text-xs text-slate-400">Live request load</div>
        </div>
      </div>

      <div className="flex justify-end mb-6">
        <button
          onClick={increasePilerRate}
          disabled={isIncreasingPiler}
          className="px-4 py-2 rounded-xl bg-blue-600 hover:bg-blue-500 text-white font-semibold disabled:opacity-60"
        >
          {isIncreasingPiler ? 'Increasing pile rate...' : `Increase pile rate (${kafkaStatus.multiplier.toFixed(1)}x)`}
        </button>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-6">
        {services.map((service) => (
          <div 
            key={service.id}
            className="bg-slate-900/40 backdrop-blur-md border border-slate-700/50 hover:border-blue-500/50 hover:bg-slate-800/40 transition-all duration-300 rounded-2xl p-6 group flex flex-col"
          >
            <div className="flex items-start justify-between mb-4">
              <div className="flex items-center gap-3">
                <div className="p-2.5 bg-slate-950 rounded-xl border border-slate-800 group-hover:border-slate-600 transition-colors">
                  {renderIcon(service.iconType)}
                </div>
                <div>
                  <h3 className="font-semibold text-slate-100">{service.name}</h3>
                  <span className="text-[10px] font-mono text-emerald-400 bg-emerald-400/10 px-2 py-0.5 rounded-full">Running</span>
                </div>
              </div>
            </div>
            
            <p className="text-sm text-slate-400 mb-6 flex-grow">{service.description}</p>
            
            <div className="space-y-4 mb-6">
              <div>
                <div className="flex justify-between text-xs text-slate-500 mb-1 font-mono">
                  <span>Current CPU</span>
                  <span className="text-slate-300">{Number(service.current.cpu || 0).toFixed(2)} Cores</span>
                </div>
                <div className="w-full bg-slate-950 rounded-full h-1.5 border border-slate-800">
                  <div className="bg-blue-500 h-1.5 rounded-full" style={{ width: `${Math.min(100, (Number(service.current.cpu || 0) / MAX_CPU) * 100)}%` }}></div>
                </div>
                <div className="mt-1 text-[10px] text-slate-400 font-mono">CPU Utilization: {Number(service.current.cpuUtilization || ((Number(service.current.cpu || 0) / MAX_CPU) * 100)).toFixed(1)}%</div>
              </div>
              <div>
                <div className="flex justify-between text-xs text-slate-500 mb-1 font-mono">
                  <span>Current RAM</span>
                  <span className="text-slate-300">{Math.round(Number(service.current.mem || 0))} MiB</span>
                </div>
                <div className="w-full bg-slate-950 rounded-full h-1.5 border border-slate-800">
                  <div className="bg-indigo-500 h-1.5 rounded-full" style={{ width: `${Math.min(100, (Number(service.current.mem || 0) / MAX_MEM) * 100)}%` }}></div>
                </div>
                <div className="mt-1 text-[10px] text-slate-400 font-mono">RAM Utilization: {Number(service.current.ramUtilization || ((Number(service.current.mem || 0) / MAX_MEM) * 100)).toFixed(1)}%</div>
              </div>
              <div className="flex items-center gap-2 text-xs text-slate-400 bg-slate-950/50 p-2 rounded-lg border border-slate-800/50">
                <BarChart3 className="w-3.5 h-3.5 text-slate-500" />
                {service.kafkaConsumer ? (
                  <>
                    Current Kafka Throughput: <span className="font-mono text-white">{Math.round(Number(service.current.mps || 0))} msg/s</span>
                  </>
                ) : (
                  <>
                    Requests served: <span className="font-mono text-white">{Math.round(Number(service.current.requestsPerSecond || 0))} req/s</span>
                  </>
                )}
              </div>
              <div className="mt-3 flex items-center gap-2 text-xs text-slate-400 bg-slate-950/50 p-2 rounded-lg border border-slate-800/50">
                <Cloud className="w-3.5 h-3.5 text-slate-500" />
                HTTPS served: <span className="font-mono text-white">{Math.round(Number(service.current.httpsRequestsServed || 0))} req/s</span>
              </div>
            </div>

            <button
              onClick={() => {
                if (!service.kafkaConsumer) return;
                setActiveService(service);
                setView('prediction');
              }}
              disabled={!service.kafkaConsumer}
              className={`w-full py-2.5 px-4 text-sm font-semibold rounded-xl transition-colors border flex items-center justify-center gap-2 ${service.kafkaConsumer ? 'bg-slate-800 hover:bg-blue-600 text-slate-200 hover:text-white border-slate-700 hover:border-blue-500' : 'bg-slate-900 text-slate-500 border-slate-800 cursor-not-allowed'}`}
            >
              <Zap className="w-4 h-4" /> {service.kafkaConsumer ? 'Optimize Resources' : 'No Kafka stream'}
            </button>
          </div>
        ))}
      </div>
    </div>
  );

  const renderPrediction = () => (
    <div>
      <div className="flex items-center gap-4 mb-8">
        <button 
          onClick={() => setView('home')}
          className="p-2 hover:bg-slate-800 rounded-lg text-slate-400 hover:text-white transition-colors border border-transparent hover:border-slate-700"
        >
          <ArrowLeft className="w-5 h-5" />
        </button>
        <div>
          <h2 className="text-xl font-bold text-white flex items-center gap-2">
            {renderIcon(activeService.iconType)}
            {activeService.name} Optimizer
          </h2>
          <p className="text-sm text-slate-400 mt-1">Simulate traffic spikes and generate recommended configurations.</p>
        </div>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-12 gap-8">
        <div className="lg:col-span-4 space-y-6">
          <div className="bg-slate-900/60 backdrop-blur-xl border border-slate-700/50 rounded-2xl p-6 shadow-2xl">
            <h2 className="text-sm font-semibold uppercase tracking-wider text-slate-400 mb-6 flex items-center gap-2">
              <Activity className="w-4 h-4" /> Workload Simulator
            </h2>

            <div className="mb-8">
              <div className="flex justify-between items-end mb-3">
                <label className="block text-xs font-medium text-slate-400">Expected Traffic (Msg/Sec)</label>
                <span className="font-mono text-lg font-bold text-blue-400 bg-blue-400/10 px-2 py-0.5 rounded-md border border-blue-400/20">
                  {mps.toLocaleString()}
                </span>
              </div>
              <input
                type="range"
                min="0"
                max="5000"
                step="50"
                value={mps}
                onChange={(e) => setMps(Number(e.target.value))}
                className="w-full h-2 bg-slate-800 rounded-lg appearance-none cursor-pointer accent-blue-500 hover:accent-blue-400 transition-all focus:outline-none"
              />
              <div className="flex justify-between text-[10px] text-slate-500 mt-2 font-mono">
                <span>0</span>
                <span>Current ({activeService.current.mps})</span>
                <span>5000</span>
              </div>
            </div>

            <button
              onClick={runPrediction}
              disabled={isPredicting}
              className="w-full relative group overflow-hidden rounded-xl p-[1px]"
            >
              <span className="absolute inset-0 bg-gradient-to-r from-blue-500 via-indigo-500 to-purple-500 rounded-xl opacity-70 group-hover:opacity-100 transition-opacity duration-300"></span>
              <div className="relative bg-slate-900 px-4 py-3.5 rounded-xl flex items-center justify-center gap-2 transition-all group-hover:bg-slate-900/80">
                {isPredicting ? (
                  <>
                    <RefreshCw className="animate-spin h-4 w-4 text-white" />
                    <span className="text-sm font-semibold text-white">Querying BigQuery ML...</span>
                  </>
                ) : (
                  <>
                    <Cloud className="w-4 h-4 text-white" />
                    <span className="text-sm font-semibold text-white">Predict Ideal Limits</span>
                  </>
                )}
              </div>
            </button>

            {errorMsg && (
              <p className="mt-3 text-xs text-rose-400 text-center">{errorMsg}</p>
            )}
          </div>
        </div>

        <div className="lg:col-span-8 space-y-6">
          <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
            <div className="bg-slate-900/60 backdrop-blur-xl border border-slate-700/50 rounded-2xl p-6 shadow-2xl relative overflow-hidden group">
              <div className="flex justify-between items-start mb-6">
                <h3 className="text-sm font-semibold text-slate-400 uppercase tracking-wider flex items-center gap-2">
                  <Cpu className="w-4 h-4" /> CPU Forecast
                </h3>
                {prediction && (
                  <span className={`px-2.5 py-1 rounded-full text-xs font-semibold flex items-center gap-1.5 bg-slate-950 border border-slate-800 ${getStatusText(prediction.cpuPercent).color}`}>
                    {getStatusText(prediction.cpuPercent).icon}
                    {getStatusText(prediction.cpuPercent).text}
                  </span>
                )}
              </div>

              <div className="space-y-4">
                <div>
                  <div className="flex justify-between text-xs text-slate-500 mb-1 font-mono">
                    <span>Current Config</span>
                    <span>{activeService.current.cpu} Cores</span>
                  </div>
                  <div className="w-full bg-slate-950 rounded-full h-2 border border-slate-800">
                    <div className="bg-slate-600 h-2 rounded-full" style={{ width: `${(activeService.current.cpu / MAX_CPU) * 100}%` }}></div>
                  </div>
                </div>

                <div>
                  <div className="flex justify-between items-baseline mb-1">
                    <span className="text-xs text-blue-400 font-medium flex items-center gap-1">
                      <Zap className="w-3 h-3" /> AI Predicted
                    </span>
                    <div className="font-mono text-white">
                      <span className="text-3xl font-bold">{prediction ? prediction.cpu : '--'}</span>
                      <span className="text-sm text-slate-500 ml-1">Cores</span>
                    </div>
                  </div>
                  <div className="w-full bg-slate-950 rounded-full h-3 border border-slate-800 overflow-hidden">
                    <div 
                      className={`h-full rounded-full transition-all duration-1000 ease-out shadow-lg ${prediction ? getStatusColor(prediction.cpuPercent) : 'bg-slate-800'}`}
                      style={{ width: `${prediction ? prediction.cpuPercent : 0}%` }}
                    ></div>
                  </div>
                </div>
              </div>
            </div>

            <div className="bg-slate-900/60 backdrop-blur-xl border border-slate-700/50 rounded-2xl p-6 shadow-2xl relative overflow-hidden group">
              <div className="flex justify-between items-start mb-6">
                <h3 className="text-sm font-semibold text-slate-400 uppercase tracking-wider flex items-center gap-2">
                  <Database className="w-4 h-4" /> Memory Forecast
                </h3>
                {prediction && (
                  <span className={`px-2.5 py-1 rounded-full text-xs font-semibold flex items-center gap-1.5 bg-slate-950 border border-slate-800 ${getStatusText(prediction.memPercent).color}`}>
                    {getStatusText(prediction.memPercent).icon}
                    {getStatusText(prediction.memPercent).text}
                  </span>
                )}
              </div>

              <div className="space-y-4">
                <div>
                  <div className="flex justify-between text-xs text-slate-500 mb-1 font-mono">
                    <span>Current Config</span>
                    <span>{activeService.current.mem} MiB</span>
                  </div>
                  <div className="w-full bg-slate-950 rounded-full h-2 border border-slate-800">
                    <div className="bg-slate-600 h-2 rounded-full" style={{ width: `${(activeService.current.mem / MAX_MEM) * 100}%` }}></div>
                  </div>
                </div>

                <div>
                  <div className="flex justify-between items-baseline mb-1">
                    <span className="text-xs text-indigo-400 font-medium flex items-center gap-1">
                      <Zap className="w-3 h-3" /> AI Predicted
                    </span>
                    <div className="font-mono text-white">
                      <span className="text-3xl font-bold">{prediction ? Number(prediction.mem).toLocaleString() : '--'}</span>
                      <span className="text-sm text-slate-500 ml-1">MiB</span>
                    </div>
                  </div>
                  <div className="w-full bg-slate-950 rounded-full h-3 border border-slate-800 overflow-hidden">
                    <div 
                      className={`h-full rounded-full transition-all duration-1000 ease-out shadow-lg ${prediction ? getStatusColor(prediction.memPercent) : 'bg-slate-800'}`}
                      style={{ width: `${prediction ? prediction.memPercent : 0}%` }}
                    ></div>
                  </div>
                </div>
              </div>
            </div>
          </div>

          <div className="bg-[#0d1117] border border-slate-700/80 rounded-2xl shadow-2xl overflow-hidden flex flex-col mt-6">
            <div className="bg-slate-900 border-b border-slate-700/80 px-4 py-3 flex items-center justify-between">
              <div className="flex items-center gap-3">
                <Terminal className="w-4 h-4 text-slate-400" />
                <span className="text-xs font-medium text-slate-300 tracking-wide">deployment.yaml</span>
              </div>
              <button 
                onClick={handleCopyYAML}
                disabled={!prediction}
                className="flex items-center gap-1.5 text-xs bg-slate-800 hover:bg-slate-700 disabled:opacity-50 disabled:hover:bg-slate-800 text-slate-300 px-3 py-1.5 rounded-lg transition-colors border border-slate-700"
              >
                {copied ? <CheckCircle2 className="w-3.5 h-3.5 text-emerald-400" /> : <Copy className="w-3.5 h-3.5" />}
                {copied ? 'Copied!' : 'Copy HPA & Deployment'}
              </button>
            </div>
            
            <div className="p-5 overflow-x-auto relative min-h-[200px]">
              {!prediction ? (
                <div className="absolute inset-0 flex items-center justify-center text-slate-600 text-sm font-medium">
                  Run a prediction to generate optimal Kubernetes manifests.
                </div>
              ) : (
                <pre className="text-[13px] font-mono leading-relaxed text-slate-300">
                  <code>
                    <span className="text-pink-400">apiVersion:</span> <span className="text-emerald-300">autoscaling/v2</span>{'\n'}
                    <span className="text-pink-400">kind:</span> <span className="text-blue-300">HorizontalPodAutoscaler</span>{'\n'}
                    <span className="text-pink-400">metadata:</span>{'\n'}
                    {'  '}<span className="text-blue-300">name:</span> <span className="text-amber-300">{activeService.id}-hpa</span>{'\n'}
                    <span className="text-slate-500">---</span>{'\n'}
                    <span className="text-pink-400">apiVersion:</span> <span className="text-emerald-300">apps/v1</span>{'\n'}
                    <span className="text-pink-400">kind:</span> <span className="text-blue-300">Deployment</span>{'\n'}
                    <span className="text-pink-400">metadata:</span>{'\n'}
                    {'  '}<span className="text-blue-300">name:</span> <span className="text-amber-300">{activeService.id}</span>{'\n'}
                    <span className="text-pink-400">spec:</span>{'\n'}
                    {'  '}<span className="text-pink-400">template:</span>{'\n'}
                    {'    '}<span className="text-pink-400">spec:</span>{'\n'}
                    {'      '}<span className="text-pink-400">containers:</span>{'\n'}
                    {'      '}- <span className="text-blue-300">name:</span> <span className="text-amber-300">{activeService.id}-app</span>{'\n'}
                    {'        '}<span className="text-pink-400">resources:</span>{'\n'}
                    {'          '}<span className="text-pink-400">requests:</span>{'\n'}
                    {'            '}<span className="text-blue-300">cpu:</span> <span className="text-amber-300">"{prediction.cpu}"</span>{'\n'}
                    {'            '}<span className="text-blue-300">memory:</span> <span className="text-amber-300">"{prediction.mem}Mi"</span>{'\n'}
                    {'          '}<span className="text-pink-400">limits:</span>{'\n'}
                    {'            '}<span className="text-blue-300">cpu:</span> <span className="text-amber-300">"{Math.min(MAX_CPU, (prediction.cpu * 1.5)).toFixed(2)}"</span>{'\n'}
                    {'            '}<span className="text-blue-300">memory:</span> <span className="text-amber-300">"{Math.min(MAX_MEM, (prediction.mem * 1.25)).toFixed(0)}Mi"</span>
                  </code>
                </pre>
              )}
            </div>
          </div>
        </div>
      </div>
    </div>
  );

  return (
    <div className="min-h-screen bg-slate-950 bg-[radial-gradient(ellipse_at_top,_var(--tw-gradient-stops))] from-slate-900 to-slate-950 text-slate-200 font-sans p-4 md:p-8">
      <header className="max-w-6xl mx-auto mb-10 flex items-center justify-between border-b border-slate-800/60 pb-6">
        <div className="flex items-center gap-4">
          <div className="p-3 bg-gradient-to-br from-blue-600 to-indigo-700 rounded-xl shadow-lg shadow-blue-900/20">
            <Cloud className="w-6 h-6 text-white" />
          </div>
          <div>
            <h1 className="text-2xl font-bold text-white tracking-tight">GCP Resource Optimizer</h1>
            <p className="text-sm text-slate-400 mt-1 flex items-center gap-2">
              <Server className="w-3.5 h-3.5" /> GKE Workload Optimization & BigQuery ML
            </p>
          </div>
        </div>
      </header>

      <main className="max-w-6xl mx-auto">
        {view === 'home' ? renderHome() : renderPrediction()}
      </main>
    </div>
  );
}