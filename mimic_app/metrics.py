"""Prometheus metric instruments used by the mimic service."""
from __future__ import annotations

import random
from typing import Iterable, List

from prometheus_client import Counter, Gauge, Histogram


def _latency_buckets_seconds(max_seconds: float = 2.0) -> Iterable[float]:
    """Generate latency buckets up to ``max_seconds`` in 100ms steps."""
    buckets: List[float] = [0.01, 0.025, 0.05, 0.075, 0.1]
    current = 0.15
    while current < max_seconds:
        buckets.append(round(current, 3))
        current += 0.1
    buckets.extend([max_seconds, max_seconds * 2, max_seconds * 4])
    return buckets


class MimicMetrics:
    """Collection of Prometheus metric instruments mirroring the JVM service."""

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


__all__ = ["MimicMetrics"]
