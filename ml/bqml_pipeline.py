import os
from google.cloud import bigquery

PROJECT_ID = os.getenv("PROJECT_ID", "your-gcp-project-id")
DATASET_ID = "gke_metrics"
TABLE_ID = "historical_usage"


def print_metric_row(title, row):
    if not row:
        print(f"{title}: N/A")
        return

    metrics = dict(row)
    for key, value in metrics.items():
        if value is None:
            continue
        print(f"{key}: {value}")


def print_query_metrics(client, title, query):
    print(f"\n=== {title} ===")
    rows = list(client.query(query).result())
    if not rows:
        print("No metrics returned.")
        return

    for row in rows:
        print_metric_row(title, row)


def train_bqml_models():
    client = bigquery.Client(project=PROJECT_ID)

    print("\n=== Dataset Summary ===")
    row_count_query = f"""
    SELECT COUNT(*) AS total_rows
    FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
    """
    print_query_metrics(client, "Dataset Row Count", row_count_query)

    print("Training CPU Linear Regression Model...")
    cpu_query = f"""
    CREATE OR REPLACE MODEL `{PROJECT_ID}.{DATASET_ID}.model_cpu`
    OPTIONS(
        model_type='LINEAR_REG',
        input_label_cols=['cpu_cores_utilized'],
        early_stop=TRUE
    ) AS
    SELECT microservice_name, messages_per_second, cpu_cores_utilized 
    FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
    """
    client.query(cpu_query).result()
    print("CPU Model trained successfully.")

    cpu_eval_query = f"""
    SELECT
      ROUND(R2_SCORE, 4) AS accuracy,
      ROUND(MEAN_SQUARED_ERROR, 4) AS mean_squared_error,
      ROUND(R2_SCORE, 4) AS r2_score,
      ROUND(MEAN_ABSOLUTE_ERROR, 4) AS mean_absolute_error
    FROM ML.EVALUATE(MODEL `{PROJECT_ID}.{DATASET_ID}.model_cpu`)
    """
    print_query_metrics(client, "CPU Model Metrics", cpu_eval_query)

    print("Training Memory Linear Regression Model...")
    mem_query = f"""
    CREATE OR REPLACE MODEL `{PROJECT_ID}.{DATASET_ID}.model_memory`
    OPTIONS(
        model_type='LINEAR_REG',
        input_label_cols=['memory_mib_utilized'],
        early_stop=TRUE
    ) AS
    SELECT microservice_name, messages_per_second, memory_mib_utilized 
    FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
    """
    client.query(mem_query).result()
    print("Memory Model trained successfully.")

    mem_eval_query = f"""
    SELECT
      ROUND(R2_SCORE, 4) AS accuracy,
      ROUND(MEAN_SQUARED_ERROR, 4) AS mean_squared_error,
      ROUND(R2_SCORE, 4) AS r2_score,
      ROUND(MEAN_ABSOLUTE_ERROR, 4) AS mean_absolute_error
    FROM ML.EVALUATE(MODEL `{PROJECT_ID}.{DATASET_ID}.model_memory`)
    """
    print_query_metrics(client, "Memory Model Metrics", mem_eval_query)

    print("\n=== Final Job Summary ===")
    model_status_query = f"""
    SELECT
      'model_cpu' AS model_name,
      training_run,
      iteration,
      loss,
      duration_ms
    FROM ML.TRAINING_INFO(MODEL `{PROJECT_ID}.{DATASET_ID}.model_cpu`)
    UNION ALL
    SELECT
      'model_memory' AS model_name,
      training_run,
      iteration,
      loss,
      duration_ms
    FROM ML.TRAINING_INFO(MODEL `{PROJECT_ID}.{DATASET_ID}.model_memory`)
    ORDER BY model_name, training_run, iteration
    """
    print_query_metrics(client, "Model Training Info", model_status_query)


if __name__ == "__main__":
    train_bqml_models()