"""Service layer orchestrating scenario generation, metrics, and Mongo writes."""
from __future__ import annotations

import random
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Iterable, List, Optional, Tuple

from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from pymongo import MongoClient
from pymongo.errors import PyMongoError

from .metrics import MimicMetrics
from .models import CollectionTarget, GenerationProfile, SinkConfig
from .mongo import MongoSinkWriter
from .scenarios import ScenarioDefinition, ScenarioSample, default_scenarios


@dataclass
class SimulationOutcome:
    """Full record of a simulation run suitable for rendering or APIs."""

    timestamp: datetime
    env: str
    topic: str
    collection: str
    kind: str
    database: str
    mongo_uri: str
    consumer_config_id: str
    scenario: ScenarioDefinition
    sample: ScenarioSample

    @property
    def successes(self) -> int:
        return self.sample.successes

    @property
    def errors(self) -> int:
        return self.sample.errors

    @property
    def records(self) -> int:
        return self.sample.total_records


@dataclass
class CollectionStatus:
    database: str
    collection: str
    kind: str
    document_count: Optional[int]
    last_metric: Optional[str]
    last_timestamp: Optional[datetime]
    error: Optional[str]

    def as_dict(self) -> Dict[str, Optional[str]]:
        return {
            "database": self.database,
            "collection": self.collection,
            "kind": self.kind,
            "documentCount": None if self.document_count is None else str(self.document_count),
            "lastMetric": self.last_metric,
            "lastTimestamp": self.last_timestamp.isoformat() if self.last_timestamp else None,
            "error": self.error,
        }


class MimicService:
    """Coordinate scenario sampling, Prometheus metrics, and Mongo writes."""

    def __init__(
        self,
        configs: List[SinkConfig],
        default_mongo_uri: str,
        scenarios: Optional[Dict[str, ScenarioDefinition]] = None,
    ) -> None:
        self._rng = random.Random()
        self._configs = configs
        self._config_index: Dict[Tuple[str, str], SinkConfig] = {
            (cfg.env, cfg.topic): cfg for cfg in configs
        }
        self._default_mongo_uri = default_mongo_uri
        self._mongo_clients: Dict[str, MongoClient] = {}
        self._writers: Dict[Tuple[str, str, str, str], MongoSinkWriter] = {}
        self._metrics = MimicMetrics()
        self._uptime = 0.0
        self._scenarios = scenarios or default_scenarios()
        self._last_result: Optional[SimulationOutcome] = None
        self._default_envs = sorted({cfg.env for cfg in configs}) or ["dev", "sit", "pat", "prod"]
        self._default_topics = sorted({cfg.topic for cfg in configs}) or [
            "orders.created",
            "payments.captured",
            "shipments.updated",
            "notifications.sent",
        ]
        collection_map: Dict[Tuple[str, str], CollectionTarget] = {}
        for cfg in configs:
            for target in cfg.collections:
                key = (target.name, target.kind)
                collection_map.setdefault(key, target)
        fallback_collections = [
            CollectionTarget(name="scalar", kind="scalar"),
            CollectionTarget(name="summary", kind="summary"),
            CollectionTarget(name="dlq_scalar", kind="scalar"),
            CollectionTarget(name="latency_summary", kind="summary"),
        ]
        defaults = list(collection_map.values())
        for option in fallback_collections:
            key = (option.name, option.kind)
            if key not in collection_map:
                defaults.append(option)
                collection_map[key] = option
            if len(defaults) >= 4:
                break
        self._default_collections = defaults[:4]

    @property
    def defaults(self) -> Dict[str, Iterable[CollectionTarget]]:
        return {
            "envs": self._default_envs,
            "topics": self._default_topics,
            "collections": self._default_collections,
        }

    @property
    def scenarios(self) -> Dict[str, ScenarioDefinition]:
        return dict(self._scenarios)

    @property
    def last_result(self) -> Optional[SimulationOutcome]:
        return self._last_result

    def _get_scenario(self, key: str) -> ScenarioDefinition:
        return self._scenarios.get(key) or next(iter(self._scenarios.values()))

    def _ensure_client(self, mongo_uri: str) -> MongoClient:
        client = self._mongo_clients.get(mongo_uri)
        if client is None:
            client = MongoClient(mongo_uri)
            self._mongo_clients[mongo_uri] = client
        return client

    def _resolve_collection_target(
        self,
        cfg: SinkConfig,
        collection_name: str,
        collection_kind: str,
    ) -> CollectionTarget:
        for target in cfg.collections:
            if target.name == collection_name and target.kind == collection_kind:
                return target
        # Default to the first known target for label propagation when available.
        label_source = cfg.collections[0] if cfg.collections else None
        return CollectionTarget(
            name=collection_name,
            kind=collection_kind,
            label_database=label_source.label_database if label_source else None,
            label_collection=label_source.label_collection if label_source else None,
        )

    def _build_config(
        self,
        env: str,
        topic: str,
        collection_name: str,
        collection_kind: str,
    ) -> Tuple[SinkConfig, CollectionTarget]:
        base = self._config_index.get((env, topic))
        if base:
            target = self._resolve_collection_target(base, collection_name, collection_kind)
            return (
                SinkConfig(
                    env=base.env,
                    topic=base.topic,
                    mongo_uri=base.mongo_uri,
                    database=base.database,
                    collections=[target],
                    generation=base.generation,
                    consumer_config_id=base.consumer_config_id,
                ),
                target,
            )
        target = CollectionTarget(name=collection_name, kind=collection_kind)
        return (
            SinkConfig(
                env=env,
                topic=topic,
                mongo_uri=self._default_mongo_uri,
                database=f"alfrd_{env}",
                collections=[target],
                generation=GenerationProfile(),
                consumer_config_id=f"{env}-{topic}".replace(" ", "_"),
            ),
            target,
        )

    def _ensure_writer(
        self,
        env: str,
        topic: str,
        collection_name: str,
        collection_kind: str,
    ) -> MongoSinkWriter:
        key = (env, topic, collection_name, collection_kind)
        writer = self._writers.get(key)
        if writer is not None:
            return writer
        config, target = self._build_config(env, topic, collection_name, collection_kind)
        client = self._ensure_client(config.mongo_uri)
        writer = MongoSinkWriter(config, client, target)
        self._writers[key] = writer
        return writer

    def simulate(
        self,
        env: str,
        topic: str,
        collection_name: str,
        collection_kind: str,
        scenario_key: str,
        records: int,
    ) -> SimulationOutcome:
        scenario = self._get_scenario(scenario_key)
        writer = self._ensure_writer(env, topic, collection_name, collection_kind)
        sample = scenario.sample(records, self._rng)

        self._record_metrics(writer, scenario, sample)

        timestamp = datetime.utcnow()
        writer.write_metrics(
            timestamp=timestamp,
            total_records=sample.total_records,
            successes=sample.successes,
            processor_latencies_ms=[int(x) for x in sample.processor_latencies_ms],
            mongo_latencies_ms=[int(x) for x in sample.mongo_latencies_ms],
            run_latencies_ms=[sample.run_latency_ms] if sample.run_latency_ms else [],
        )

        result = SimulationOutcome(
            timestamp=timestamp,
            env=env,
            topic=topic,
            collection=collection_name,
            kind=collection_kind,
            database=writer.database_name,
            mongo_uri=writer.config.mongo_uri,
            consumer_config_id=writer.config.consumer_config_id,
            scenario=scenario,
            sample=sample,
        )
        self._last_result = result
        return result

    def _record_metrics(
        self,
        writer: MongoSinkWriter,
        scenario: ScenarioDefinition,
        sample: ScenarioSample,
    ) -> None:
        topic = writer.config.topic
        consumer_id = writer.config.consumer_config_id
        metric_db = writer.metric_database
        metric_coll = writer.metric_collection

        self._metrics.kafka_fetched.labels(topic="").inc(sample.total_records)
        self._metrics.kafka_fetched.labels(topic=topic).inc(sample.total_records)
        self._metrics.kafka_records_out.labels(topic="").inc(sample.successes)
        self._metrics.kafka_records_out.labels(topic=topic).inc(sample.successes)

        if sample.total_records:
            self._metrics.kafka_topic_duration.labels(topic=topic).observe(sample.run_latency_ms / 1000.0)
            self._metrics.kafka_running_duration.observe(sample.run_latency_ms / 1000.0)

        if sample.errors:
            self._metrics.kafka_topic_errors.labels(topic=topic).inc(sample.errors)
            if self._rng.random() < scenario.consumer_error_probability:
                self._metrics.kafka_consumer_errors.labels(consumerConfigId=consumer_id).inc(1)
            if self._rng.random() < scenario.kafka_error_probability:
                self._metrics.kafka_errors.inc(1)

        self._metrics.processor_success.labels(topic=topic).inc(sample.successes)
        for stage, count in sample.failure_breakdown.items():
            if count:
                self._metrics.processor_failure.labels(topic=topic, failure_stage=stage).inc(count)
        if sample.processor_latencies_ms:
            for latency in sample.processor_latencies_ms:
                self._metrics.processor_duration.labels(topic=topic).observe(latency / 1000.0)

        if sample.successes:
            self._metrics.mongo_success.labels(database=metric_db, collection=metric_coll).inc(sample.successes)
            for latency in sample.mongo_latencies_ms[:20]:
                self._metrics.mongo_success_duration.labels(
                    database=metric_db,
                    collection=metric_coll,
                ).observe(latency / 1000.0)
        if sample.errors:
            self._metrics.mongo_failure_routed.labels(database=metric_db, collection=metric_coll).inc(sample.errors)
            for latency in sample.failure_latencies_ms[:10]:
                self._metrics.mongo_failure_duration.labels(
                    database=metric_db,
                    collection=metric_coll,
                ).observe(latency / 1000.0)

        self._uptime += max(1.0, sample.total_records / max(writer.config.generation.records_per_second, 1.0))
        self._metrics.update_jvm_metrics(self._uptime)

    def get_collection_status(self) -> List[CollectionStatus]:
        statuses: List[CollectionStatus] = []
        for writer in self._writers.values():
            collection = writer.collection
            try:
                count = collection.count_documents({})
                latest = collection.find_one(sort=[("timestamp", -1)])
                last_metric = latest.get("metricName") if latest else None
                last_ts = latest.get("timestamp") if latest else None
                if last_ts is not None and not isinstance(last_ts, datetime):
                    try:
                        last_ts = datetime.fromisoformat(str(last_ts))
                    except (TypeError, ValueError):
                        last_ts = None
                statuses.append(
                    CollectionStatus(
                        database=writer.database_name,
                        collection=writer.collection_name,
                        kind=writer.collection_kind,
                        document_count=count,
                        last_metric=last_metric,
                        last_timestamp=last_ts,
                        error=None,
                    )
                )
            except PyMongoError as exc:
                statuses.append(
                    CollectionStatus(
                        database=writer.database_name,
                        collection=writer.collection_name,
                        kind=writer.collection_kind,
                        document_count=None,
                        last_metric=None,
                        last_timestamp=None,
                        error=str(exc),
                    )
                )
        statuses.sort(key=lambda item: (item.database, item.collection))
        return statuses

    def generate_metrics_response(self) -> Tuple[bytes, str]:
        payload = generate_latest()
        return payload, CONTENT_TYPE_LATEST

    def close(self) -> None:
        for client in self._mongo_clients.values():
            client.close()
        self._mongo_clients.clear()


__all__ = ["CollectionStatus", "MimicService", "SimulationOutcome"]
