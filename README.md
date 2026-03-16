# Airflow Concurrent API Execution Patterns

This repository serves as a testing ground and reference architecture for executing high-volume external API calls concurrently within Google Cloud Composer (or any Apache Airflow 2.x environment).

It demonstrates the evolution from simple, inefficient synchronous loops to highly scalable, resilient asynchronous paradigms utilizing Dynamic Task Mapping and Deferrable Operators (Triggers).

## Overview

We simulate pinging 500 individual external endpoints (mocked using simple sleeps in configurations sourced from a YAML file). The repository provides seven different DAGs to demonstrate the trade-offs of various concurrency implementations.

## Getting Started

1. **Generate Mock Configurations:**
   Run the native Python script to generate the synthetic `api_calls.yaml` configuration file. This file dictates the wait times (1 second per call).
   ```bash
   python dags/generate_yaml.py
   ```

2. **Load the DAGs:**
   Place the `dags/` folder directly into your Airflow/Composer `$DAGS_FOLDER`.

## Exploring the Implementation Patterns

The implemented DAGs are scaled logically from worst practice (synchronous loop) to best practice (Deferrable / Chunked Async).

1. **`simulate_api_sync`** (`dag_sync.py`)
   - **Pattern:** A single `PythonOperator` loops synchronously through all 500 calls.
   - **Pros:** Extremely simple.
   - **Cons:** Blocks the worker slot for 500+ seconds. A single failure kills the task. Requires re-running all prior successful requests on failure.

2. **`simulate_api_sync_split`** (`dag_sync_split.py`)
   - **Pattern:** A Python `for`-loop at the root level dynamically creates 500 separate `PythonOperator` definitions at parse-time.
   - **Pros:** 1-to-1 observability natively in the UI graph. Perfect retry granularity.
   - **Cons:** Stresses the metadata DB and scheduler excessively. Unnecessary graph clutter.

3. **`simulate_api_dynamic`** (`dag_dynamic.py`)
   - **Pattern:** Utilizes Airflow >=2.3 Dynamic Task Mapping (`.expand()`).
   - **Pros:** Excellent native Airflow 2.x feature; cleaner UI than manual loop splitting.
   - **Cons:** Still creates 500 distinct row entries in the metadata database. 

4. **`simulate_api_async`** (`dag_async.py`)
   - **Pattern:** Uses a single Python operator running purely native `asyncio.sleep()` / `asyncio.gather()`. Runs all 500 calls simultaneously.
   - **Pros:** Lightning fast. Completes 500 1-second sleeps in roughly 1 second using only 1 worker slot runtime.
   - **Cons:** No UI observability per call. If the worker encounters an Out Of Memory (OOM) error while parsing thousands of JSON responses concurrently, the entire process dies. "Fat task" anti-pattern.

5. **`simulate_api_async_custom_operator`** (`dag_async_operator.py`)
   - **Pattern:** Wraps the `asyncio` logic into a reusable `BaseOperator`. Implements graceful internal exception catching and exponential backoff retry isolation (disables Airflow retries).
   - **Pros:** Exceptionally robust against transient network errors.

6. **`simulate_api_async_chunked`** (`dag_async_chunked.py`)
   - **Pattern:** **The Production Hybrid Standard**. Batches the 500 calls into chunks of 50. Uses Airflow Dynamic Mapping `.expand()` to map the asynchronous custom operator instance 10 times.
   - **Pros:** Scales perfectly. Balances DB metadata load (only 10 Airflow tasks) with memory safety (caps async tasks per node to 50) and retains isolated chunk retries in the UI.

7. **`simulate_api_deferrable`** (`dag_deferrable.py`)
   - **Pattern:** Uses Airflow native Deferrable Operators (`BaseTrigger`).
   - **Pros:** True asynchronous execution explicitly coordinated by Airflow. Suspends the worker instantly. Uses `0` active worker slots while waiting for external I/O polling, unlocking near-infinite concurrency.
   - **Cons:** Requires Airflow 2.2+ Triggerer instances running in the cluster. Complex to author.

## Architecture Guidelines

If you intend to use this repository to design your own data stack:
* If you have <1,000 requests to make: Use **Dynamic Mapping** (`dag_dynamic.py`).
* If you have 100,000+ requests to make: Use **Chunked Async Mapping** (`dag_async_chunked.py`).
* If your API calls take >60 seconds to respond per request: Use **Deferrable Operators** (`dag_deferrable.py`).

## Deployment

### 1. Standard Airflow / Google Cloud Composer
To deploy these DAGs, you simply need to upload the `dags/` folder contents into your environment's designated DAGs bucket/folder.

For Google Cloud Composer:
```bash
# Upload all python scripts and the yaml configuration
gcloud storage cp dags/* gs://<YOUR_COMPOSER_BUCKET>/dags/
```

### 2. Enabling Deferrable Operators (Triggers)
The `simulate_api_deferrable` DAG requires an active Airflow **Triggerer** process running in your cluster.

**On Google Cloud Composer 2.x:**
By default, the Triggerer component is *not* enabled to save costs. You must explicitly configure at least 1 triggerer instance.
```bash
gcloud composer environments update <ENVIRONMENT_NAME> \
    --location <LOCATION> \
    --triggerer-count 1 \
    --triggerer-cpu 0.5 \
    --triggerer-memory 1GB
```

**On Local / Open-Source Airflow:**
When starting your Airflow services, ensure you run the triggerer CLI command alongside your scheduler and webserver:
```bash
airflow triggerer
```
If you are using Docker Compose (e.g., the official `docker-compose.yaml`), make sure the `airflow-triggerer` service is uncommented and running.
