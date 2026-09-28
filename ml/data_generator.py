import os
import numpy as np
import pandas as pd
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

PROJECT_ID = os.getenv("PROJECT_ID", "your-gcp-project-id")
DATASET_ID = "gke_metrics"
TABLE_ID = "historical_usage"
REGION = os.getenv("REGION", "us-central1")
N_SAMPLES = 10000

def generate_telemetry_data(n_samples: int) -> pd.DataFrame:
    """
    Synthesize realistic microservice message throughput vs. resource consumption.
    Includes baseline consumption, linear scaling, non-linear traffic bursts, and jitter.
    """
    np.random.seed(42)

    # 1. Simulate varying incoming messages/sec with traffic waves and spikes
    time_series = np.linspace(0, 50, n_samples)
    diurnal_pattern = np.sin(time_series) * 400 + 600
    burst_events = np.random.choice([0, 500, 1200], size=n_samples, p=[0.85, 0.12, 0.03])
    noise = np.random.normal(0, 60, n_samples)
    messages_per_sec = np.clip(diurnal_pattern + burst_events + noise, 20, 2500).astype(int)

    # 2. Simulate CPU Cores Utilized
    # Base idle CPU + linear consumption per message + exponential stress under saturation (>1400 msg/s)
    base_cpu = 0.15
    linear_cpu = messages_per_sec * 0.0018
    saturation_penalty = np.where(messages_per_sec > 1400, (messages_per_sec - 1400) ** 1.3 * 0.0008, 0.0)
    cpu_noise = np.random.normal(0, 0.05, n_samples)
    cpu_cores = np.clip(base_cpu + linear_cpu + saturation_penalty + cpu_noise, 0.1, 8.0)

    # 3. Simulate Memory (MiB) Utilized
    # Base JVM/container overhead + memory allocation per in-flight batch + garbage collection lag
    base_mem = 384
    mem_scaling = messages_per_sec * 0.65
    mem_noise = np.random.normal(0, 45, n_samples)
    memory_mib = np.clip(base_mem + mem_scaling + mem_noise, 256, 8192).astype(int)

    # 4. Construct DataFrame
    timestamps = pd.date_range(start="2026-08-01", periods=n_samples, freq="1min")
    services = np.random.choice(["order-processor", "payment-gateway", "notification-worker"], size=n_samples)

    df = pd.DataFrame({
        "timestamp": timestamps,
        "microservice_name": services,
        "messages_per_second": messages_per_sec,
        "cpu_cores_utilized": np.round(cpu_cores, 3),
        "memory_mib_utilized": memory_mib
    })

    return df

def upload_to_bigquery(df: pd.DataFrame, project_id: str, dataset_id: str, table_id: str):
    client = bigquery.Client(project=project_id)

    # Ensure dataset exists
    dataset_ref = bigquery.DatasetReference(project_id, dataset_id)
    try:
        client.get_dataset(dataset_ref)
        print(f"Dataset '{dataset_id}' exists.")
    except NotFound:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = REGION
        client.create_dataset(dataset)
        print(f"Created BigQuery dataset '{dataset_id}' in {REGION}.")

    full_table_path = f"{project_id}.{dataset_id}.{table_id}"

    # Explicit schema definition matching BigQuery and Vertex AI Tabular expectations
    schema = [
        bigquery.SchemaField("timestamp", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("microservice_name", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("messages_per_second", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("cpu_cores_utilized", "FLOAT", mode="REQUIRED"),
        bigquery.SchemaField("memory_mib_utilized", "INTEGER", mode="REQUIRED"),
    ]

    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
    )

    print(f"Uploading {len(df)} telemetry records to BigQuery: {full_table_path} ...")
    job = client.load_table_from_dataframe(df, full_table_path, job_config=job_config)
    job.result()  # Wait for the load job to complete
    print(f"Successfully loaded {job.output_rows} rows into {full_table_path}.")

if __name__ == "__main__":
    print("Generating synthetic GKE telemetry data...")
    telemetry_df = generate_telemetry_data(N_SAMPLES)
    upload_to_bigquery(telemetry_df, PROJECT_ID, DATASET_ID, TABLE_ID)