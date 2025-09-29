"""MongoDB writers used by the Alfred mimic application."""
from __future__ import annotations

import sys
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

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
        self.base_tags = {
            "hostname": socket.gethostname(),
            "appName": DEFAULT_APP_NAME,
            "topic": config.topic,
            "env": config.env,
        }

    def write_metrics(
        self,
        timestamp: datetime,
        total_records: int,
        successes: int,
        errors: int,
        duplicates: int,
        processor_latencies_ms: List[int],
        mongo_latencies_ms: List[int],
        run_latencies_ms: List[int],
        destination_database: str,
        destination_collection: str,
    ) -> None:
        processor_tags = self._compose_tags(include_destination=False)
        destination_tags = self._compose_tags(destination_database, destination_collection)
        duplicate_tags = self._compose_tags(
            destination_database,
            self._derive_global_collection(destination_collection),
        )
        if self.collection_kind == "summary":
            docs = self._build_summary_docs(
                timestamp,
                processor_latencies_ms,
                mongo_latencies_ms,
                run_latencies_ms,
                processor_tags,
                destination_tags,
            )
        else:
            docs = self._build_scalar_docs(
                timestamp,
                total_records,
                successes,
                errors,
                duplicates,
                processor_tags,
                destination_tags,
                duplicate_tags,
            )
        if not docs:
            return
        try:
            self.collection.insert_many(docs, ordered=False)
        except PyMongoError as exc:  # pragma: no cover - defensive logging
            print(
                f"[mongo] Failed to insert metrics for {self.config.topic}: {exc}",
                file=sys.stderr,
            )

    def _build_scalar_docs(
        self,
        timestamp: datetime,
        total_records: int,
        successes: int,
        errors: int,
        duplicates: int,
        processor_tags: Dict[str, Any],
        destination_tags: Dict[str, Any],
        duplicate_tags: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        records = max(int(total_records), 0)
        success_count = max(int(successes), 0)
        error_count = max(int(errors), 0)
        duplicate_count = max(int(duplicates), 0)

        docs: List[Dict[str, Any]] = []

        docs.append(
            self._counter_doc(
                timestamp,
                "alfrd_kafka_to_mongo_sink_records_count",
                records,
                processor_tags,
            )
        )
        docs.append(
            self._counter_doc(
                timestamp,
                "alfrd_kafka_record_processor_success_records_count",
                success_count,
                processor_tags,
            )
        )
        if success_count > 0:
            docs.append(
                self._counter_doc(
                    timestamp + timedelta(seconds=5),
                    "alfrd_kafka_record_processor_success_records_count",
                    0,
                    processor_tags,
                    extra_tags={"interval": "idle"},
                )
            )
        docs.append(
            self._counter_doc(
                timestamp,
                "alfrd_mongodb_saver_success_records_count",
                success_count,
                destination_tags,
            )
        )
        if success_count > 0:
            docs.append(
                self._counter_doc(
                    timestamp + timedelta(seconds=10),
                    "alfrd_mongodb_saver_success_records_count",
                    0,
                    destination_tags,
                    extra_tags={"interval": "idle"},
                )
            )
        docs.append(
            self._counter_doc(
                timestamp,
                "alfrd_mongodb_saver_failure_handling_records_count",
                error_count,
                destination_tags,
                extra_tags={"routing": "failure"},
            )
        )
        docs.append(
            self._counter_doc(
                timestamp + timedelta(seconds=2),
                "alfrd_mongodb_saver_global_only_records_count",
                duplicate_count,
                duplicate_tags,
                extra_tags={"routing": "global_only"},
            )
        )
        return docs

    def _build_summary_docs(
        self,
        timestamp: datetime,
        processor_latencies_ms: List[int],
        mongo_latencies_ms: List[int],
        run_latencies_ms: List[int],
        processor_tags: Dict[str, Any],
        destination_tags: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        docs: List[Dict[str, Any]] = []

        docs.append(
            self._histogram_doc(
                timestamp,
                "alfrd_kafka_record_processor_processing_duration",
                processor_latencies_ms,
                processor_tags,
            )
        )
        docs.append(
            self._histogram_doc(
                timestamp,
                "alfrd_mongodb_saver_success_saving_duration",
                mongo_latencies_ms,
                destination_tags,
            )
        )
        docs.append(
            self._histogram_doc(
                timestamp,
                "alfrd_kafka_to_mongo_sink_running_duration",
                run_latencies_ms,
                processor_tags,
            )
        )
        return docs

    def _histogram_doc(
        self,
        timestamp: datetime,
        metric_name: str,
        samples_ms: List[int],
        tags: Dict[str, Any],
    ) -> Dict[str, Any]:
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
            "tags": dict(tags),
        }

    def _counter_doc(
        self,
        timestamp: datetime,
        metric_name: str,
        value: int,
        tags: Dict[str, Any],
        *,
        extra_tags: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        if value > 0:
            min_value = 1
            max_value = value
            count_value = value
        else:
            min_value = INT64_MAX
            max_value = INT64_MIN
            count_value = 0
        payload_tags = dict(tags)
        if extra_tags:
            payload_tags.update(extra_tags)
        return {
            "timestamp": timestamp,
            "metricName": metric_name,
            "metricType": "counter",
            "value": value,
            "count": count_value,
            "sum": value,
            "min": min_value,
            "max": max_value,
            "tags": payload_tags,
        }

    def _compose_tags(
        self,
        destination_database: Optional[str] = None,
        destination_collection: Optional[str] = None,
        *,
        include_destination: bool = True,
        extra: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        tags = dict(self.base_tags)
        if include_destination:
            tags["database"] = (destination_database or self.metric_database).upper()
            tags["collection"] = destination_collection or self.metric_collection
        if extra:
            tags.update(extra)
        return tags

    @staticmethod
    def _derive_global_collection(destination_collection: Optional[str]) -> str:
        base = (destination_collection or "global_metrics").strip()
        if not base:
            return "global_metrics"
        lowered = base.lower()
        if lowered.startswith("global"):
            return base
        return f"global::{base}"


__all__ = ["MongoSinkWriter"]
