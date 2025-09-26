# Alfred Mimic Metric Testbed

This repository contains a lightweight "mimic" application that synthesises the
Prometheus metrics produced by the Alfred Kafka → Mongo pipeline. It fans four
Kafka topics into `Observability.scalar`/`Observability.summary` while tagging
each record as if it belonged to the source domain databases (e.g.
`ABPAY.abpay_raw_collection`). Use it alongside your front-end metric dashboards
to exercise throughput, latency, and JVM health scenarios without connecting to
real Kafka or Mongo clusters.

## Repository layout

```
.
├── config/
│   └── mimic-topology.yml       # Default topology: 4 topics mirrored to scalar/summary collections
├── docker-compose.mongodb.yml   # Spins up dev/sit/pat/prod MongoDB containers
├── mongo-init/
│   └── observability-timeseries.js  # Example init script that can create time-series collections
├── mimic_app/                   # Python mimic generator package
│   └── main.py
└── requirements.txt             # Python dependencies
```

## Prerequisites

- Docker Engine 24+ (Docker Desktop or Homebrew `docker`).
- Docker Compose plugin 2.18+.

When Docker is installed via Homebrew, the Compose plugin resides in
`/opt/homebrew/lib/docker/cli-plugins`. Make sure Docker can discover it by
adding `cliPluginsExtraDirs` to `~/.docker/config.json`:

```json
{
  "cliPluginsExtraDirs": [
    "/opt/homebrew/lib/docker/cli-plugins"
  ]
}
```

Verify Compose is available:

```bash
docker compose version
```

### Colima users

If you run Docker through Colima, ensure your shell uses the Colima context:

```bash
colima start
docker context ls
docker context create colima --docker host=unix:///Users/$(whoami)/.colima/default/docker.sock  # one time
docker context use colima
```

Repeat the final command whenever a new shell defaults back to the `default`
context.

## 1. Start the MongoDB tier

Create a shared Docker network once:

```bash
docker network create alfred-mimic-net
```

The repository already includes `docker-compose.mongodb.yml` which provisions
four isolated MongoDB containers (dev, sit, pat, prod). Launch them with:

```bash
docker compose -f docker-compose.mongodb.yml up -d
```

> ⚠️ If port `27017` is already bound (for example by an SSH tunnel), stop the
> conflicting process (`lsof -iTCP:27017 -sTCP:LISTEN`) or change the host port
> mapping for `mongo-dev` in `docker-compose.mongodb.yml`.

Each instance exposes its MongoDB port on the host for inspection while also
being discoverable by the mimic app via the Docker network hostnames
(`mongo-dev`, `mongo-sit`, `mongo-pat`, `mongo-prod`). Data persists in the
`mongo-*-data/` folders.

On first boot any scripts under `mongo-init/` run automatically. Adapt
`observability-timeseries.js` (or replace it) if you need Mongo to pre-create
collections before the mimic starts. If you launched the stack before the file
existed, delete the `mongo-*-data/` directories (or run `docker compose ... down
-v`) so the init script can run against a fresh data volume.

### Optional: seed credentials

If you need authentication, create a shared application user in each container:

```bash
docker exec -it mongo-dev mongosh -u root -p root --authenticationDatabase admin --eval "db.getSiblingDB('admin').createUser({user: 'alfred', pwd: 'alfred', roles: ['readWriteAnyDatabase']})"
docker exec -it mongo-sit mongosh -u root -p root --authenticationDatabase admin --eval "db.getSiblingDB('admin').createUser({user: 'alfred', pwd: 'alfred', roles: ['readWriteAnyDatabase']})"
docker exec -it mongo-pat mongosh -u root -p root --authenticationDatabase admin --eval "db.getSiblingDB('admin').createUser({user: 'alfred', pwd: 'alfred', roles: ['readWriteAnyDatabase']})"
docker exec -it mongo-prod mongosh -u root -p root --authenticationDatabase admin --eval "db.getSiblingDB('admin').createUser({user: 'alfred', pwd: 'alfred', roles: ['readWriteAnyDatabase']})"
```

> Credentials live inside each Mongo data volume. If you wipe the volumes (for
> example `docker compose -f docker-compose.mongodb.yml down -v` or deleting
> `mongo-*-data/`), rerun the commands above before connecting as `alfred`.

## 2. Configure topics, collections, and generation profiles

The mimic application reads `config/mimic-topology.yml`. The checked-in file
defines four topics mapped to the per-environment MongoDB instances (dev, sit,
pat, prod). Each
sink entry controls throughput, error behaviour, and latency distributions.

```yaml
defaults:
  generation:
    recordsPerSecond: 600
    burstRecords: 1200
    errorRate: 0.003
    latencyMs:
      p50: 55
      p95: 140
      p99: 320

sinks:
  - env: dev
    topic: abpay_pdm
    mongoUri: mongodb://alfred:alfred@localhost:27017/?authSource=admin
    database: Observability
    collections:
      - name: scalar
        kind: scalar
        labelDatabase: ABPAY
        labelCollection: abpay_pdm_collection
      - name: summary
        kind: summary
        labelDatabase: ABPAY
        labelCollection: abpay_pdm_collection_summary
    generation:
      recordsPerSecond: 550
      latencyMs:
        p50: 45
        p95: 110
        p99: 260

  - env: sit
    topic: abpay_raw
    mongoUri: mongodb://alfred:alfred@localhost:27018/?authSource=admin
    database: Observability
    collections:
      - name: scalar
        kind: scalar
        labelDatabase: ABPAY
        labelCollection: abpay_raw_collection
      - name: summary
        kind: summary
        labelDatabase: ABPAY
        labelCollection: abpay_raw_collection_summary
    generation:
      recordsPerSecond: 750
      errorRate: 0.004
      latencyMs:
        p50: 65
        p95: 150
        p99: 340

  - env: pat
    topic: eftr_tds_raw
    mongoUri: mongodb://alfred:alfred@localhost:27019/?authSource=admin
    database: Observability
    collections:
      - name: scalar
        kind: scalar
        labelDatabase: EFTR
        labelCollection: eftr_tds_raw_collection
      - name: summary
        kind: summary
        labelDatabase: EFTR
        labelCollection: eftr_tds_raw_collection_summary
    generation:
      recordsPerSecond: 480
      errorRate: 0.007
      latencyMs:
        p50: 80
        p95: 190
        p99: 420

  - env: prod
    topic: eftr_tds_summary
    mongoUri: mongodb://alfred:alfred@localhost:27020/?authSource=admin
    database: Observability
    collections:
      - name: scalar
        kind: scalar
        labelDatabase: EFTR
        labelCollection: eftr_tds_summary_collection
      - name: summary
        kind: summary
        labelDatabase: EFTR
        labelCollection: eftr_tds_summary_collection_summary
    generation:
      recordsPerSecond: 650
      errorRate: 0.002
      latencyMs:
        p50: 70
        p95: 160
        p99: 360
```

The sample configuration keeps writes in `Observability.scalar` and
`Observability.summary` while labelling each metric as if it belonged to the
application-facing databases (for example `ABPAY.abpay_pdm_collection`). The `mongoUri`
entries point at the host-exposed ports (`localhost:27017` for dev, `27018` for
sit, `27019` for pat, `27020` for prod) so the Python process can connect from
your workstation. If you run the mimic inside the Docker network instead, swap
them back to the internal hostnames (`mongo-dev`, `mongo-sit`, `mongo-pat`,
`mongo-prod`). To add more
scenarios (for example prod fan-out or additional metric shapes), duplicate a
sink entry, point `topic`, `mongoUri`, and `collections` at the desired targets,
and optionally tune the generation parameters:

* `recordsPerSecond` – steady-state throughput for the topic.
* `burstRecords` – extra records injected occasionally to simulate spikes.
* `errorRate` – portion of records routed through failure handling.
* `latencyMs` – percentile targets that shape the histogram distributions.

## 3. Install dependencies and run the mimic generator

Create a Python environment (3.9+) and install the requirements:

```bash
# First-time setup
python3 -m venv .venv
# Activate for this shell
source .venv/bin/activate
pip install -r requirements.txt
```

If activation reports `no such file or directory`, the `.venv` folder does not
exist—rerun the `python3 -m venv .venv` step in the current directory.

Launch the generator. By default it reads `config/mimic-topology.yml`, updates
metrics every second, and exposes them on `http://localhost:8000/metrics`:

```bash
python -m mimic_app.main
```

(`mimic_app` currently has no `__main__.py`, so running `python -m mimic_app`
will report that the package cannot be executed directly.)

If startup fails with `Address already in use`, another process is bound to the
metrics port (default `8000`). Find and stop it with
`lsof -iTCP:8000 -sTCP:LISTEN` or launch the mimic on a new port, e.g.
`python -m mimic_app.main --port 9000`.

Keep the process running in that terminal—the mimic logs
`Serving Alfred mimic metrics on http://0.0.0.0:8000/metrics`. If you stop it or
close the shell, the `/metrics` endpoint disappears.

Environment variables can override the defaults:

* `MIMIC_CONFIG` – path to a topology file.
* `MIMIC_PORT` – metrics port (default `8000`).
* `MIMIC_INTERVAL` – update cadence in seconds (default `1.0`).

You can also pass the same flags directly, for example:

```bash
python -m mimic_app --config ./config/mimic-topology.yml --port 9000 --interval 0.5
```

Stop the generator with <kbd>Ctrl</kbd>+<kbd>C</kbd>.

## 4. Validate the exported metrics

1. Use `mongosh` or MongoDB Compass to confirm each container is reachable,
   e.g. `mongosh 'mongodb://alfred:alfred@localhost:27017/?authSource=admin'`
   (quote the URI in shells like zsh so `?` is not treated as a glob).
   If `mongosh` is missing locally, install it with `brew install mongosh` or
  run `docker exec -it mongo-dev mongosh` instead. Once connected, run
  `use Observability; show collections;` to verify the mimic has created the
  time-series collections.
2. With the mimic process still running, fetch the metrics via
   `curl http://localhost:8000/metrics` (or your chosen `--port`) and look for:
   * `alfrd_kafka_to_mongo_sink_fetched_records_total{topic="..."}` and
     `topic=""` (aggregate) for backlog views.
   * `alfrd_kafka_record_processor_success_records_count_total` and
     `..._failure_records_count_total{failure_stage="..."}` for validation flow.
   * `alfrd_mongodb_saver_success_saving_duration_bucket` and
     `..._failure_handling_duration_bucket` histograms for persistence latency.
   * JVM-style gauges such as `jvm_memory_used_bytes`, `process_cpu_usage`, and
     `system_cpu_usage` for host health dashboards.
   If the request fails, confirm the mimic process is still running, the port
   value matches the `--port` flag, and no firewall or VPN blocks localhost.
3. Validate MongoDB ingestion per environment while the generator is running:
   * Dev metrics (port 27017): query `Observability.scalar` and
     `Observability.summary` (documents carry `database=ABPAY` tags for
     throughput/latency respectively).
   * SIT metrics (port 27018): same collections with `database=ABPAY` but
     `topic=abpay_raw` tags.
   * PAT metrics (port 27019): same collections with `database=EFTR` and
     `topic=eftr_tds_raw` tags.
   * PROD metrics (port 27020): same collections with
     `database=EFTR` and `topic=eftr_tds_summary` tags.
   Example command:
   `mongosh 'mongodb://root:root@localhost:27020/?authSource=admin' --eval "db.getSiblingDB('Observability').summary.find({\"tags.collection\": \"eftr_tds_summary_collection_summary\"}).limit(3)"`
   Each query should return documents shaped like the samples in
   `resources/METRICS.scalar` or `resources/METRICS.summary`. If a query returns
   nothing, confirm the mimic process is still running, the `alfred` user exists
   on that Mongo instance, and watch the mimic logs for `[mongo] Failed to insert…`
   messages.
4. Front-end integration guide (see `docs/frontend-integration.md` for the full playbook):
   * **Collections & scope** – Every environment writes into
     `Observability.scalar` / `Observability.summary`; documents and Prometheus
     series expose realistic `database` / `collection` labels such as
     `ABPAY.abpay_raw_collection`, so frontends can reuse production dashboards
     without modification.
   * **Kafka → Mongo sink orchestration**
     - `alfrd_kafka_to_mongo_sink_fetched_records_total{topic="..."}` (counter)
       shows topic-level backlog, plus the empty-label series for global totals.
     - `alfrd_kafka_to_mongo_sink_records_count_total{topic="..."}` (counter)
       reports fan-out writes. Compare with processor success counts to detect
       duplication/drop.
     - `alfrd_kafka_to_mongo_sink_topic_duration` (histogram) captures poll →
       commit latency per topic; use `buckets`/`count`/`sum` to compute p95/p99.
     - `alfrd_kafka_to_mongo_sink_running_duration` (histogram) tracks loop
       cadence across all topics.
     - `alfrd_kafka_to_mongo_sink_topic_errors_total{topic}` (counter),
       `alfrd_kafka_to_mongo_sink_consumer_errors_total{consumerConfigId}`, and
       `alfrd_kafka_to_mongo_sink_errors_total` provide failure telemetry.
   * **Kafka record processor**
     - `alfrd_kafka_record_processor_success_records_count_total{topic}` and
       `..._failure_records_count_total{topic,failure_stage}` report per-stage
       throughput/validation outcomes (stages: json_structure_validation,
       retrieve_from_schema_registry, transformation, deserialization, unknown).
     - `alfrd_kafka_record_processor_processing_duration` (histogram) isolates
       transformation latency per topic independent of Mongo writes.
   * **Mongo saver**
     - `alfrd_mongodb_saver_success_records_count_total{database,collection}` and
       `..._failure_handling_records_count_total{database,collection}` show write
       vs retry volumes.
     - `alfrd_mongodb_saver_success_saving_duration` /
       `..._failure_handling_duration` (histograms) expose insert latencies and
       failure-handling times per collection.
   * **System/JVM overlays** – The mimic exports Micrometer-style gauges such as
     `jvm_memory_used_bytes`, `jvm_threads_live_threads`, `process_cpu_usage`,
     `system_cpu_usage`, and `process_files_open_files` to support host-health
     panels alongside pipeline metrics.
   * **Document schema** – Counter documents include `value`, `count`, `sum`,
     `min`, `max`, and `tags` (topic/env/hostname/appName). Histogram documents
     add `buckets` and `percentiles` fields matching the layout in
     `resources/METRICS.summary`. Dashboards can group by `tags.topic`,
     `tags.env`, `database`, `collection`, and `failure_stage` for drilldowns.
   * **Query tips** – Pull the most recent datapoint per metric using
     `find().sort({timestamp:-1}).limit(1)`, or aggregate over `timestamp` for
     time-series charts. Histograms can be converted to percentiles with
     `buckets`/`count`/`sum` or precomputed `percentiles` values.
5. Adjust throughput, burst, or error values in the topology file to create the
   scenarios you want to test (sustained lag, error storms, latency spikes).

When finished, stop the Mongo tier and optionally clear the data directories:

```bash
docker compose -f docker-compose.mongodb.yml down
rm -rf mongo-*-data
```

Your frontend can now connect to the mimic endpoint to validate dashboards and
alerting logic across the Kafka poller, record processor, Mongo saver, and JVM
metric families.
