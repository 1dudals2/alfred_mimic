import argparse
import asyncio
import math
import os
import random
import signal
import socket
import sys
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional

import yaml
from prometheus_client import Counter, Gauge, Histogram, start_http_server
from pymongo import MongoClient
from pymongo.errors import PyMongoError


INT64_MAX = 2**63 - 1
INT64_MIN = -2**63
HISTOGRAM_BUCKETS_MS = [
    50,
    100,
    200,
    300,
    500,
    750,
    1000,
    1500,
    2000,
    2250,
    2500,
    2750,
    3000,
    3500,
    4000,
    5000,
]
DEFAULT_APP_NAME = "alfred-mimic"


def _quantile(sorted_samples: List[float], percentile: float) -> int:
    if not sorted_samples:
        return 0
    if len(sorted_samples) == 1:
        return int(round(sorted_samples[0]))
    position = (len(sorted_samples) - 1) * percentile
    lower_idx = math.floor(position)
    upper_idx = math.ceil(position)
    lower = sorted_samples[lower_idx]
    upper = sorted_samples[upper_idx]
    if lower_idx == upper_idx:
        return int(round(lower))
    interpolated = lower + (upper - lower) * (position - lower_idx)
    return int(round(interpolated))


def _build_bucket_counts(samples: List[int]) -> Dict[str, int]:
    if not samples:
        return {str(boundary): 0 for boundary in HISTOGRAM_BUCKETS_MS}
    sorted_samples = sorted(samples)
    result: Dict[str, int] = {}
    idx = 0
    for boundary in HISTOGRAM_BUCKETS_MS:
        while idx < len(sorted_samples) and sorted_samples[idx] <= boundary:
            idx += 1
        result[str(boundary)] = idx
    return result


FAILURE_STAGES = [
    "json_structure_validation",
    "retrieve_from_schema_registry",
    "transformation",
    "deserialization",
    "unknown",
]


def _latency_buckets_seconds(max_seconds: float = 2.0) -> Iterable[float]:
    """Generate latency buckets up to `max_seconds` in 100ms steps."""
    buckets: List[float] = [0.01, 0.025, 0.05, 0.075, 0.1]
    current = 0.15
    while current < max_seconds:
        buckets.append(round(current, 3))
        current += 0.1
    buckets.extend([max_seconds, max_seconds * 2, max_seconds * 4])
    return buckets


@dataclass
class LatencyProfile:
    p50: float = 50.0
    p95: float = 150.0
    p99: float = 350.0

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "LatencyProfile":
        return cls(
            p50=float(data.get("p50", 50.0)),
            p95=float(data.get("p95", data.get("p90", 150.0))),
            p99=float(data.get("p99", data.get("p999", 350.0))),
        )


@dataclass
class GenerationProfile:
    records_per_second: float = 500.0
    burst_records: int = 0
    error_rate: float = 0.0
    latency: LatencyProfile = field(default_factory=LatencyProfile)

    @classmethod
    def from_dict(cls, defaults: Dict[str, Any], override: Optional[Dict[str, Any]]) -> "GenerationProfile":
        base = dict(defaults or {})
        override = override or {}
        latency = LatencyProfile.from_dict({**base.get("latencyMs", {}), **override.get("latencyMs", {})})
        return cls(
            records_per_second=float(override.get("recordsPerSecond", base.get("recordsPerSecond", 500.0))),
            burst_records=int(override.get("burstRecords", base.get("burstRecords", 0))),
            error_rate=float(override.get("errorRate", base.get("errorRate", 0.0))),
            latency=latency,
        )


@dataclass
class CollectionTarget:
    name: str
    kind: str = "scalar"
    label_database: Optional[str] = None
    label_collection: Optional[str] = None

    @classmethod
    def from_any(cls, value: Any) -> "CollectionTarget":
        if isinstance(value, str):
            name = value.strip()
            if not name:
                raise ValueError("Collection name cannot be empty.")
            return cls(name=name)
        if isinstance(value, dict):
            name_raw = value.get("name")
            if name_raw is None:
                raise ValueError("Collection entry is missing 'name'.")
            name = str(name_raw).strip()
            if not name:
                raise ValueError("Collection name cannot be empty.")
            kind_raw = value.get("kind") or value.get("type") or "scalar"
            kind = str(kind_raw).strip().lower() or "scalar"
            if kind not in {"scalar", "summary"}:
                raise ValueError(f"Unsupported collection kind '{kind}'.")
            label_database_raw = value.get("labelDatabase") or value.get("metricDatabase")
            label_collection_raw = value.get("labelCollection") or value.get("metricCollection")
            label_database = str(label_database_raw).strip() if label_database_raw else None
            label_collection = str(label_collection_raw).strip() if label_collection_raw else None
            return cls(name=name, kind=kind, label_database=label_database, label_collection=label_collection)
        raise TypeError("Collection entries must be strings or mappings with 'name'.")


@dataclass
class SinkConfig:
    env: str
    topic: str
    mongo_uri: str
    database: str
    collections: List[CollectionTarget]
    generation: GenerationProfile
    consumer_config_id: str

    @property
    def collection(self) -> str:
        return self.collections[0].name if self.collections else "events"

    @classmethod
    def from_dict(cls, defaults: Dict[str, Any], data: Dict[str, Any]) -> "SinkConfig":
        generation_defaults = defaults.get("generation", {})
        generation = GenerationProfile.from_dict(generation_defaults, data.get("generation"))
        env = data.get("env") or data.get("name") or data.get("topic", "sink").split(".")[-1]
        consumer_id = data.get("consumerConfigId") or f"{env}-{data.get('topic', 'topic')}"
        collection_field = data.get("collections")
        if collection_field is None:
            collection_field = data.get("collection", "events")
        if isinstance(collection_field, str) or isinstance(collection_field, dict):
            raw_collections = [collection_field]
        else:
            try:
                raw_collections = list(collection_field)
            except TypeError as exc:
                raise TypeError("collections must be a string, mapping, or an iterable of those") from exc
        collections: List[CollectionTarget] = []
        seen: set[tuple[str, str]] = set()
        for entry in raw_collections:
            if entry is None:
                continue
            target = CollectionTarget.from_any(entry)
            key = (target.name, target.kind)
            if key in seen:
                continue
            seen.add(key)
            collections.append(target)
        if not collections:
            collections = [CollectionTarget(name="events", kind="scalar")]
        return cls(
            env=env,
            topic=data["topic"],
            mongo_uri=data.get("mongoUri", "mongodb://localhost:27017"),
            database=data.get("database", f"alfrd_{env}"),
            collections=collections,
            generation=generation,
            consumer_config_id=consumer_id,
        )


class MongoSinkWriter:
    def __init__(self, config: SinkConfig, client: MongoClient, target: CollectionTarget) -> None:
        self.config = config
        self.database_name = config.database
        self.target = target
        self.collection_name = target.name
        self.collection_kind = target.kind
        self.collection = client[config.database][self.collection_name]
        self.metric_database = target.label_database or config.database
        self.metric_collection = target.label_collection or self.collection_name
        self.tags = {
            "hostname": socket.gethostname(),
            "appName": DEFAULT_APP_NAME,
            "topic": config.topic,
            "env": config.env,
            "database": self.metric_database,
            "collection": self.metric_collection,
        }

    def write_metrics(
        self,
        timestamp: datetime,
        total_records: int,
        successes: int,
        processor_latencies_ms: List[int],
        mongo_latencies_ms: List[int],
        run_latencies_ms: List[int],
    ) -> None:
        if self.collection_kind == "summary":
            docs = self._build_summary_docs(timestamp, processor_latencies_ms, mongo_latencies_ms, run_latencies_ms)
        else:
            docs = self._build_scalar_docs(timestamp, total_records, successes)
        if not docs:
            return
        try:
            self.collection.insert_many(docs, ordered=False)
        except PyMongoError as exc:  # pylint: disable=broad-except
            print(f"[mongo] Failed to insert metrics for {self.config.topic}: {exc}", file=sys.stderr)

    def _build_scalar_docs(self, timestamp: datetime, total_records: int, successes: int) -> List[Dict[str, Any]]:
        records = max(int(total_records), 0)
        success_count = max(int(successes), 0)
        docs: List[Dict[str, Any]] = []

        def make_doc(metric_name: str, value: int) -> Dict[str, Any]:
            if value > 0:
                min_value = 1
                max_value = value
            else:
                min_value = INT64_MAX
                max_value = INT64_MIN
            return {
                "timestamp": timestamp,
                "metricName": metric_name,
                "metricType": "counter",
                "value": value,
                "count": value,
                "sum": value,
                "min": min_value,
                "max": max_value,
                "tags": dict(self.tags),
            }

        docs.append(make_doc("alfrd_kafka_to_mongo_sink_records_count", records))
        docs.append(make_doc("alfrd_kafka_record_processor_success_records_count", success_count))
        docs.append(make_doc("alfrd_mongodb_saver_success_records_count", success_count))
        return docs

    def _build_summary_docs(
        self,
        timestamp: datetime,
        processor_latencies_ms: List[int],
        mongo_latencies_ms: List[int],
        run_latencies_ms: List[int],
    ) -> List[Dict[str, Any]]:
        docs: List[Dict[str, Any]] = []

        docs.append(self._histogram_doc(
            timestamp,
            "alfrd_kafka_record_processor_processing_duration",
            processor_latencies_ms,
        ))
        docs.append(self._histogram_doc(
            timestamp,
            "alfrd_mongodb_saver_success_saving_duration",
            mongo_latencies_ms,
        ))
        docs.append(self._histogram_doc(
            timestamp,
            "alfrd_kafka_to_mongo_sink_running_duration",
            run_latencies_ms,
        ))
        return docs

    def _histogram_doc(self, timestamp: datetime, metric_name: str, samples_ms: List[int]) -> Dict[str, Any]:
        if samples_ms:
            sorted_samples = sorted(samples_ms)
            count = len(sorted_samples)
            sum_value = int(round(sum(sorted_samples)))
            min_value = int(sorted_samples[0])
            max_value = int(sorted_samples[-1])
            percentiles = {
                "p50": _quantile(sorted_samples, 0.50),
                "p90": _quantile(sorted_samples, 0.90),
                "p95": _quantile(sorted_samples, 0.95),
                "p99": _quantile(sorted_samples, 0.99),
            }
            # Remove zero percentiles to mimic real exporter behaviour
            percentiles = {k: v for k, v in percentiles.items() if v is not None}
        else:
            count = 0
            sum_value = 0
            min_value = INT64_MAX
            max_value = INT64_MIN
            percentiles = {}

        buckets = _build_bucket_counts(samples_ms)

        return {
            "timestamp": timestamp,
            "metricName": metric_name,
            "metricType": "histogram",
            "count": count,
            "sum": sum_value,
            "min": min_value,
            "max": max_value,
            "buckets": buckets,
            "percentiles": percentiles,
            "tags": dict(self.tags),
        }
class MimicMetrics:
    def __init__(self) -> None:
        latency_buckets = _latency_buckets_seconds()

        # Kafka → Mongo sink metrics
        self.kafka_fetched = Counter(
            "alfrd_kafka_to_mongo_sink_fetched_records_total",
            "Total Kafka records fetched per topic.",
            labelnames=("topic",),
        )
        self.kafka_records_out = Counter(
            "alfrd_kafka_to_mongo_sink_records_count_total",
            "Records written to Mongo per topic.",
            labelnames=("topic",),
        )
        self.kafka_topic_duration = Histogram(
            "alfrd_kafka_to_mongo_sink_topic_duration",
            "Topic batch end-to-end latency in seconds.",
            labelnames=("topic",),
            buckets=list(latency_buckets),
        )
        self.kafka_running_duration = Histogram(
            "alfrd_kafka_to_mongo_sink_running_duration",
            "Run loop latency in seconds.",
            buckets=list(latency_buckets),
        )
        self.kafka_topic_errors = Counter(
            "alfrd_kafka_to_mongo_sink_topic_errors_total",
            "Errors encountered while saving or committing a topic batch.",
            labelnames=("topic",),
        )
        self.kafka_consumer_errors = Counter(
            "alfrd_kafka_to_mongo_sink_consumer_errors_total",
            "Consumer-level exceptions.",
            labelnames=("consumerConfigId",),
        )
        self.kafka_errors = Counter(
            "alfrd_kafka_to_mongo_sink_errors_total",
            "Top-level run loop errors.",
        )

        # Processor metrics
        self.processor_success = Counter(
            "alfrd_kafka_record_processor_success_records_count_total",
            "Successful processor records per topic.",
            labelnames=("topic",),
        )
        self.processor_failure = Counter(
            "alfrd_kafka_record_processor_failure_records_count_total",
            "Failed processor records per topic and stage.",
            labelnames=("topic", "failure_stage"),
        )
        self.processor_duration = Histogram(
            "alfrd_kafka_record_processor_processing_duration",
            "Processor latency per topic in seconds.",
            labelnames=("topic",),
            buckets=list(latency_buckets),
        )

        # Mongo saver metrics
        self.mongo_success = Counter(
            "alfrd_mongodb_saver_success_records_count_total",
            "Successful Mongo writes per database and collection.",
            labelnames=("database", "collection"),
        )
        self.mongo_failure_routed = Counter(
            "alfrd_mongodb_saver_failure_handling_records_count_total",
            "Records routed to failure handling paths.",
            labelnames=("database", "collection"),
        )
        self.mongo_success_duration = Histogram(
            "alfrd_mongodb_saver_success_saving_duration",
            "Mongo insert latency in seconds.",
            labelnames=("database", "collection"),
            buckets=list(latency_buckets),
        )
        self.mongo_failure_duration = Histogram(
            "alfrd_mongodb_saver_failure_handling_duration",
            "Failure handling latency in seconds.",
            labelnames=("database", "collection"),
            buckets=list(latency_buckets),
        )

        # JVM/system gauges (synthetic)
        self.jvm_memory_used = Gauge(
            "jvm_memory_used_bytes",
            "Simulated JVM memory usage.",
            labelnames=("area", "id"),
        )
        self.jvm_threads_live = Gauge("jvm_threads_live_threads", "Simulated live threads count.")
        self.jvm_threads_daemon = Gauge("jvm_threads_daemon_threads", "Simulated daemon threads count.")
        self.jvm_threads_peak = Gauge("jvm_threads_peak_threads", "Simulated peak threads count.")
        self.process_cpu_usage = Gauge("process_cpu_usage", "Simulated process CPU usage fraction.")
        self.process_uptime = Gauge("process_uptime_seconds", "Simulated process uptime.")
        self.system_cpu_usage = Gauge("system_cpu_usage", "Simulated system CPU usage fraction.")
        self.process_files_open = Gauge("process_files_open_files", "Simulated open file descriptors.")
        self.process_files_max = Gauge("process_files_max_files", "Simulated max file descriptors.")
        self._peak_threads = 0

    def update_jvm_metrics(self, uptime_seconds: float) -> None:
        heap_used = 300 * 1024 * 1024 + random.uniform(-50, 80) * 1024 * 1024
        nonheap_used = 80 * 1024 * 1024 + random.uniform(-10, 20) * 1024 * 1024
        self.jvm_memory_used.labels(area="heap", id="G1 Eden Space").set(max(heap_used, 64 * 1024 * 1024))
        self.jvm_memory_used.labels(area="heap", id="G1 Old Gen").set(max(heap_used * 0.4, 32 * 1024 * 1024))
        self.jvm_memory_used.labels(area="nonheap", id="CodeHeap").set(max(nonheap_used * 0.3, 16 * 1024 * 1024))
        self.jvm_memory_used.labels(area="nonheap", id="Metaspace").set(max(nonheap_used * 0.6, 24 * 1024 * 1024))

        live_threads = int(40 + random.gauss(0, 5))
        daemon_threads = int(live_threads * 0.75)
        self._peak_threads = max(self._peak_threads, live_threads)
        self.jvm_threads_live.set(max(live_threads, 10))
        self.jvm_threads_daemon.set(max(daemon_threads, 5))
        self.jvm_threads_peak.set(self._peak_threads)

        self.process_cpu_usage.set(max(min(random.uniform(0.05, 0.35), 1.0), 0.0))
        self.system_cpu_usage.set(max(min(random.uniform(0.15, 0.85), 1.0), 0.0))
        self.process_uptime.set(uptime_seconds)
        self.process_files_open.set(120 + random.randint(-10, 15))
        self.process_files_max.set(1024)


class SinkSimulator:
    def __init__(
        self,
        config: SinkConfig,
        metrics: MimicMetrics,
        writers: List[MongoSinkWriter],
        interval: float = 1.0,
    ) -> None:
        if not writers:
            raise ValueError(f"Sink {config.topic} must have at least one collection")
        self.config = config
        self.metrics = metrics
        self.writers = writers
        self.interval = interval
        self._uptime = 0.0

    def _sample_records(self) -> int:
        base = self.config.generation.records_per_second * self.interval
        jitter = random.gauss(0, base * 0.05)
        count = max(base + jitter, 0)
        if self.config.generation.burst_records and random.random() < 0.05:
            count += self.config.generation.burst_records
        return int(max(count, 0))

    def _sample_errors(self, records: int) -> int:
        if records == 0:
            return 0
        mean = records * self.config.generation.error_rate
        stddev = math.sqrt(max(mean * (1 - self.config.generation.error_rate), 1.0))
        value = int(random.gauss(mean, stddev))
        return int(max(0, min(value, records)))

    def _sample_latency_seconds(self) -> float:
        profile = self.config.generation.latency
        # Piecewise distribution approximating percentile targets
        roll = random.random()
        if roll < 0.5:
            return max(random.uniform(profile.p50 * 0.5, profile.p50 * 1.2) / 1000.0, 0.001)
        if roll < 0.95:
            return random.uniform(profile.p50, profile.p95) / 1000.0
        if roll < 0.99:
            return random.uniform(profile.p95, profile.p99) / 1000.0
        return random.uniform(profile.p99, profile.p99 * 1.6) / 1000.0

    async def run(self) -> None:
        topic = self.config.topic
        db = self.config.database
        consumer_id = self.config.consumer_config_id
        while True:
            records = self._sample_records()
            errors = self._sample_errors(records)
            successes = max(records - errors, 0)

            processor_samples_ms: List[int] = []
            mongo_samples_ms: List[int] = []
            if successes > 0:
                sample_count = min(successes, 50)
                processor_samples_ms = [
                    int(round(self._sample_latency_seconds() * 1000.0)) for _ in range(sample_count)
                ]
                mongo_samples_ms = [
                    int(round(self._sample_latency_seconds() * 1000.0)) for _ in range(sample_count)
                ]
            run_duration_ms = int(round(self._sample_latency_seconds() * 1000.0))

            # Kafka sink counters ("" topic label reserved for global totals)
            self.metrics.kafka_fetched.labels(topic="").inc(records)
            self.metrics.kafka_fetched.labels(topic=topic).inc(records)
            self.metrics.kafka_records_out.labels(topic="").inc(successes)
            self.metrics.kafka_records_out.labels(topic=topic).inc(successes)

            if records:
                self.metrics.kafka_topic_duration.labels(topic=topic).observe(self._sample_latency_seconds())
                self.metrics.kafka_running_duration.observe(self._sample_latency_seconds())

            if errors:
                self.metrics.kafka_topic_errors.labels(topic=topic).inc(errors)
                if random.random() < 0.1:
                    self.metrics.kafka_consumer_errors.labels(consumerConfigId=consumer_id).inc(1)
                if random.random() < 0.02:
                    self.metrics.kafka_errors.inc(1)

            # Processor metrics
            self.metrics.processor_success.labels(topic=topic).inc(successes)
            if errors:
                remaining = errors
                for stage in FAILURE_STAGES:
                    if remaining <= 0:
                        break
                    slice_size = max(0, int(random.random() * remaining))
                    if stage == FAILURE_STAGES[-1]:
                        slice_size = remaining
                    self.metrics.processor_failure.labels(topic=topic, failure_stage=stage).inc(slice_size)
                    remaining -= slice_size
            self.metrics.processor_duration.labels(topic=topic).observe(self._sample_latency_seconds())

            # Mongo saver metrics
            if successes:
                for writer in self.writers:
                    metric_db = writer.metric_database
                    metric_coll = writer.metric_collection
                    self.metrics.mongo_success.labels(database=metric_db, collection=metric_coll).inc(successes)
                    for _ in range(min(successes, 20)):
                        self.metrics.mongo_success_duration.labels(database=metric_db, collection=metric_coll).observe(
                            self._sample_latency_seconds()
                        )
            if errors:
                for writer in self.writers:
                    metric_db = writer.metric_database
                    metric_coll = writer.metric_collection
                    self.metrics.mongo_failure_routed.labels(database=metric_db, collection=metric_coll).inc(errors)
                    for _ in range(min(errors, 10)):
                        self.metrics.mongo_failure_duration.labels(database=metric_db, collection=metric_coll).observe(
                            self._sample_latency_seconds() * 1.3
                        )

            self._uptime += self.interval
            self.metrics.update_jvm_metrics(self._uptime)

            for writer in self.writers:
                writer.write_metrics(
                    timestamp=datetime.utcnow(),
                    total_records=records,
                    successes=successes,
                    processor_latencies_ms=processor_samples_ms,
                    mongo_latencies_ms=mongo_samples_ms,
                    run_latencies_ms=[run_duration_ms] if run_duration_ms else [],
                )

            await asyncio.sleep(self.interval)


def load_configuration(path: str) -> List[SinkConfig]:
    with open(path, "r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle)
    if not data or "sinks" not in data:
        raise ValueError("Configuration must define at least one sink entry under 'sinks'.")
    defaults = data.get("defaults", {})
    sinks = [SinkConfig.from_dict(defaults, sink) for sink in data["sinks"]]
    if not sinks:
        raise ValueError("No sinks defined in configuration.")
    return sinks


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Alfred metric mimic generator")
    parser.add_argument(
        "--config",
        default=os.environ.get("MIMIC_CONFIG", "config/mimic-topology.yml"),
        help="Path to the topology configuration file.",
    )
    parser.add_argument("--port", type=int, default=int(os.environ.get("MIMIC_PORT", "8000")), help="Metrics port.")
    parser.add_argument(
        "--interval",
        type=float,
        default=float(os.environ.get("MIMIC_INTERVAL", "1.0")),
        help="Seconds between metric updates.",
    )
    return parser.parse_args()


async def _run_simulation(
    configs: List[SinkConfig], writers_by_sink: List[List[MongoSinkWriter]], interval: float
) -> None:
    metrics = MimicMetrics()
    tasks = [
        SinkSimulator(cfg, metrics, writer_group, interval).run()
        for cfg, writer_group in zip(configs, writers_by_sink)
    ]
    await asyncio.gather(*tasks)


def main() -> None:
    args = parse_args()
    try:
        configs = load_configuration(args.config)
    except Exception as exc:  # pylint: disable=broad-except
        print(f"Failed to load configuration: {exc}", file=sys.stderr)
        sys.exit(1)

    mongo_clients: Dict[str, MongoClient] = {}
    writers_by_sink: List[List[MongoSinkWriter]] = []
    for cfg in configs:
        client = mongo_clients.setdefault(cfg.mongo_uri, MongoClient(cfg.mongo_uri))
        writer_group = [MongoSinkWriter(cfg, client, collection) for collection in cfg.collections]
        writers_by_sink.append(writer_group)

    start_http_server(args.port)
    print(f"Serving Alfred mimic metrics on http://0.0.0.0:{args.port}/metrics")

    loop = asyncio.get_event_loop()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, loop.stop)
        except NotImplementedError:
            pass

    try:
        loop.create_task(_run_simulation(configs, writers_by_sink, args.interval))
        loop.run_forever()
    finally:
        loop.close()
        for client in set(mongo_clients.values()):
            client.close()


if __name__ == "__main__":
    main()
