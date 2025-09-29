"""MongoDB writers used by the Alfred mimic application."""
from __future__ import annotations

import sys
from datetime import datetime
from typing import Any, Dict, List

import socket

from pymongo import MongoClient
from pymongo.errors import PyMongoError

from .models import (
    DEFAULT_APP_NAME,
    INT64_MAX,
    INT64_MIN,
    CollectionTarget,
    SinkConfig,
    build_bucket_counts,
    compute_quantile,
)


class MongoSinkWriter:
    """Persist synthetic metrics to MongoDB collections."""

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
            docs = self._build_summary_docs(
                timestamp,
                processor_latencies_ms,
                mongo_latencies_ms,
                run_latencies_ms,
            )
        else:
            docs = self._build_scalar_docs(timestamp, total_records, successes)
        if not docs:
            return
        try:
            self.collection.insert_many(docs, ordered=False)
        except PyMongoError as exc:  # pragma: no cover - defensive logging
            print(
                f"[mongo] Failed to insert metrics for {self.config.topic}: {exc}",
                file=sys.stderr,
            )

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

        docs.append(
            self._histogram_doc(
                timestamp,
                "alfrd_kafka_record_processor_processing_duration",
                processor_latencies_ms,
            )
        )
        docs.append(
            self._histogram_doc(
                timestamp,
                "alfrd_mongodb_saver_success_saving_duration",
                mongo_latencies_ms,
            )
        )
        docs.append(
            self._histogram_doc(
                timestamp,
                "alfrd_kafka_to_mongo_sink_running_duration",
                run_latencies_ms,
            )
        )
        return docs

    def _histogram_doc(self, timestamp: datetime, metric_name: str, samples_ms: List[int]) -> Dict[str, Any]:
        if samples_ms:
            sorted_samples = sorted(samples_ms)
            count = len(sorted_samples)
            sum_value = int(round(sum(sorted_samples)))
            min_value = int(sorted_samples[0])
            max_value = int(sorted_samples[-1])
            percentiles = {
                "p50": compute_quantile(sorted_samples, 0.50),
                "p90": compute_quantile(sorted_samples, 0.90),
                "p95": compute_quantile(sorted_samples, 0.95),
                "p99": compute_quantile(sorted_samples, 0.99),
            }
            percentiles = {k: v for k, v in percentiles.items() if v is not None}
        else:
            count = 0
            sum_value = 0
            min_value = INT64_MAX
            max_value = INT64_MIN
            percentiles = {}

        buckets = build_bucket_counts(samples_ms)

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


__all__ = ["MongoSinkWriter"]
