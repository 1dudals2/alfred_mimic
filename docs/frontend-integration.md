# Alfred Mimic → Frontend Integration Guide

This document explains how a frontend or dashboarding application can consume the
synthetic metrics produced by the Alfred Mimic project. It covers connectivity,
collection layout, document schema, metric catalogue, and typical query patterns
so you can visualise the Kafka → Mongo ETL pipeline end-to-end.

## 1. Runtime topology recap

| Environment | Docker container | Host port | Mongo URI                                     | Stored collections | Metric labels present |
|-------------|------------------|-----------|-----------------------------------------------|--------------------|-----------------------|
| Dev         | `mongo-dev`      | 27017     | `mongodb://alfred:alfred@localhost:27017/?authSource=admin` | `Observability.scalar`, `Observability.summary` | `database=ABPAY`, `collection=abpay_pdm_collection[_summary]` |
| SIT         | `mongo-sit`      | 27018     | `mongodb://alfred:alfred@localhost:27018/?authSource=admin` | `Observability.scalar`, `Observability.summary` | `database=ABPAY`, `collection=abpay_raw_collection[_summary]` |
| PAT         | `mongo-pat`      | 27019     | `mongodb://alfred:alfred@localhost:27019/?authSource=admin` | `Observability.scalar`, `Observability.summary` | `database=EFTR`, `collection=eftr_tds_raw_collection[_summary]` |
| PROD        | `mongo-prod`     | 27020     | `mongodb://alfred:alfred@localhost:27020/?authSource=admin` | `Observability.scalar`, `Observability.summary` | `database=EFTR`, `collection=eftr_tds_summary_collection[_summary]` |

* Authentication – all instances ship with `alfred / alfred` (read-write). If you
  recreate Mongo volumes, re-run the seed commands in `README.md` (§1 → Optional:
  seed credentials).
* Data retention – Mimic writes continuously while `python -m mimic_app.main` is
  running. Stop the process to freeze data generation.

## 2. Collections and schema

Two Mongo time-series collections hold the metric documents:

* `Observability.scalar`
  * Counter-style metrics – cumulative totals for throughput and error counts.
  * `tags.database` / `tags.collection` reflect the source system (e.g. `ABPAY.abpay_pdm_collection`).
  * Documents follow this structure:
    ```json
    {
      "timestamp": ISODate("2025-09-25T02:53:33.215Z"),
      "metricName": "alfrd_kafka_to_mongo_sink_records_count",
      "metricType": "counter",
      "value": 538,
      "count": 538,
      "sum": 538,
      "min": 1,
      "max": 538,
      "tags": {
        "hostname": "MacBook-Pro-3.local",
        "appName": "alfred-mimic",
        "topic": "abpay_pdm",
        "env": "dev",
        "database": "ABPAY",
        "collection": "abpay_pdm_collection"
      }
    }
    ```

* `Observability.summary`
  * Histogram metrics – latency distributions plus pre-computed percentiles.
  * Documents share the same `tags.database` / `tags.collection` convention for realistic dashboards.
  * Documents follow this structure:
    ```json
    {
      "timestamp": ISODate("2025-09-25T02:57:09.341Z"),
      "metricName": "alfrd_kafka_record_processor_processing_duration",
      "metricType": "histogram",
      "count": 50,
      "sum": 5046,
      "min": 37,
      "max": 425,
      "buckets": {
        "50": 7,
        "100": 34,
        "200": 47,
        "300": 48,
        "500": 50,
        "750": 50,
        "1000": 50,
        "1500": 50,
        "2000": 50,
        "2250": 50,
        "2500": 50,
        "2750": 50,
        "3000": 50,
        "3500": 50,
        "4000": 50,
        "5000": 50
      },
      "percentiles": {
        "p50": 85,
        "p90": 145,
        "p95": 197,
        "p99": 366
      },
      "tags": {
        "hostname": "MacBook-Pro-3.local",
        "appName": "alfred-mimic",
        "topic": "eftr_tds_summary",
        "env": "prod",
        "database": "EFTR",
        "collection": "eftr_tds_summary_collection_summary"
      }
    }
    ```

> Tip: Mongo automatically creates bucket collections (`system.buckets.*`), so
> you only need to query `scalar` or `summary` directly.

## 3. Metric catalogue

### 3.1 Kafka → Mongo sink (orchestration layer)

| Metric | Type | Labels | Description / Dashboard usage |
|--------|------|--------|--------------------------------|
| `alfrd_kafka_to_mongo_sink_fetched_records_total` | Counter | `topic` (empty label for total) | Kafka backlog fetched per topic and overall. Drives backlog charts. |
| `alfrd_kafka_to_mongo_sink_records_count_total` | Counter | `topic` | Records emitted from the sink. Compare against processor success counts. |
| `alfrd_kafka_to_mongo_sink_topic_duration` | Histogram | `topic` | Poll → commit latency for each topic batch. Use bucket data for p95/p99. |
| `alfrd_kafka_to_mongo_sink_running_duration` | Histogram | – | Sink loop cadence. Detect scheduling drift. |
| `alfrd_kafka_to_mongo_sink_topic_errors_total` | Counter | `topic` | Errors while saving/committing a topic batch. |
| `alfrd_kafka_to_mongo_sink_consumer_errors_total` | Counter | `consumerConfigId` | Exceptions escaping the consumer loop (env-topic identifier). |
| `alfrd_kafka_to_mongo_sink_errors_total` | Counter | – | Run-level loop failures. Any spike indicates the sink aborted. |

### 3.2 Kafka record processor (transformation/validation)

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `alfrd_kafka_record_processor_success_records_count_total` | Counter | `topic` | Successful records per topic before fan-out. |
| `alfrd_kafka_record_processor_failure_records_count_total` | Counter | `topic`, `failure_stage` | Failures by stage (`json_structure_validation`, `retrieve_from_schema_registry`, `transformation`, `deserialization`, `unknown`). |
| `alfrd_kafka_record_processor_processing_duration` | Histogram | `topic` | CPU time spent per processor batch, independent of Mongo latency. |

### 3.3 Mongo saver (persistence)

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `alfrd_mongodb_saver_success_records_count_total` | Counter | `database`, `collection` | Successful Mongo writes. Highlights fan-out distribution. |
| `alfrd_mongodb_saver_failure_handling_records_count_total` | Counter | `database`, `collection` | Records routed to retry/catch-all flows. |
| `alfrd_mongodb_saver_success_saving_duration` | Histogram | `database`, `collection` | Insert latency distribution. |
| `alfrd_mongodb_saver_failure_handling_duration` | Histogram | `database`, `collection` | Retry/catch-all latency distribution. |

### 3.4 JVM / system health (Micrometer synthetic gauges)

| Metric | Type | Labels | Notes |
|--------|------|--------|-------|
| `jvm_memory_used_bytes` | Gauge | `area`, `id` | Heap/non-heap usage across pools. |
| `jvm_threads_live_threads`, `jvm_threads_peak_threads`, `jvm_threads_daemon_threads` | Gauge | – | Thread utilisation. |
| `process_cpu_usage`, `system_cpu_usage` | Gauge | – | CPU usage of process vs host. |
| `process_uptime_seconds` | Gauge | – | Running time of mimic process. |
| `process_files_open_files`, `process_files_max_files` | Gauge | – | File descriptor usage. |

## 4. Query recipes

### 4.1 Latest snapshot per metric

Fetch the latest counter for a topic:
```js
// Dev example – most recent sink records for abpay_pdm
use Observability;
db.scalar.find({
  metricName: "alfrd_kafka_to_mongo_sink_records_count",
  "tags.collection": "abpay_pdm_collection"
}).sort({ timestamp: -1 }).limit(1);
```

### 4.2 Time-series panel (grouped by time)

Use `$group` to bucket by minute and compute deltas:
```js
// Throughput per minute (dev)
use Observability;
db.scalar.aggregate([
  { $match: { metricName: "alfrd_kafka_to_mongo_sink_records_count", "tags.collection": "abpay_pdm_collection" } },
  { $sort: { timestamp: 1 } },
  { $group: {
      _id: { $toDate: { $subtract: [ { $toLong: "$timestamp" }, { $mod: [ { $toLong: "$timestamp" }, 60000 ] } ] } },
      firstValue: { $first: "$value" },
      lastValue: { $last: "$value" }
  }},
  { $project: { ts: "$_id", ratePerMin: { $subtract: [ "$lastValue", "$firstValue" ] } } },
  { $sort: { ts: 1 } }
]);
```

### 4.3 Latency percentiles from histograms

```js
// Latest PAT Mongo insert latency percentiles
use Observability;
db.summary.find({
  metricName: "alfrd_mongodb_saver_success_saving_duration",
  "tags.collection": "eftr_tds_raw_collection_summary"
}).sort({ timestamp: -1 }).limit(1);
```
Use `percentiles.p95`/`p99` directly or rebuild quantiles with `buckets` and
`count` if you prefer PromQL-style calculations.

### 4.4 Error dashboards

Counter rollups for failure stages or Mongo retries:
```js
// Processor failures by stage (SIT)
use Observability;
db.scalar.aggregate([
  { $match: { metricName: "alfrd_kafka_record_processor_failure_records_count", "tags.collection": "abpay_raw_collection" } },
  { $group: { _id: "$tags.failure_stage", totalFailures: { $max: "$value" } } },
  { $sort: { totalFailures: -1 } }
]);
```

## 5. Dashboard blueprint

1. **Backlog panel** – line chart from `alfrd_kafka_to_mongo_sink_fetched_records_total`
   (global + per topic). Use `rate()` or derivative.
2. **Processor throughput** – compare `..._success_records_count_total` vs
   `..._failure_records_count_total` stacked by `failure_stage`.
3. **Sink health** – table summarising topic errors, consumer errors, and
   `..._errors_total` to flag run-level failures.
4. **Mongo latency** – percentile chart from
   `alfrd_mongodb_saver_success_saving_duration` and
   `..._failure_handling_duration` per collection.
5. **System health** – small multiples for `process_cpu_usage`,
   `jvm_memory_used_bytes`, `jvm_threads_live_threads`, etc.

## 6. Troubleshooting

* **No documents returned** – ensure mimic is running (`python -m mimic_app.main ...`).
  Every fetch should show logs like `Serving Alfred mimic metrics on ...`.
* **Authentication failure** – rerun the user creation snippet in the README when
  volumes are recreated (credentials live inside the Mongo data directories).
* **Empty percentiles** – histograms will set `count = 0`, `min = 2^63-1` until
  at least one sample is recorded; wait a few seconds or verify throughput is
  non-zero.
* **Topic name mismatch** – restart the mimic process after editing
  `config/mimic-topology.yml`; it reads configuration at launch.

## 7. Reference

* Sample documents: `resources/METRICS.scalar`, `resources/METRICS.summary`.
* Runtime configuration: `config/mimic-topology.yml`.
* Mongo init script: `mongo-init/observability-timeseries.js` (creates collections
  and indexes).

With these details, a frontend can confidently connect to the mimic Mongo
instances, understand the metric taxonomy, and render observability dashboards
covering backlog, throughput, latency, errors, and platform health.
