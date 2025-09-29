"""Core data structures and helpers for the Alfred mimic application."""
from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional


INT64_MAX = 2**63 - 1
INT64_MIN = -2**63

HISTOGRAM_BUCKETS_MS: List[int] = [
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

FAILURE_STAGES = [
    "json_structure_validation",
    "retrieve_from_schema_registry",
    "transformation",
    "deserialization",
    "unknown",
]


def compute_quantile(sorted_samples: List[float], percentile: float) -> int:
    """Return the interpolated percentile of the provided samples."""
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


def build_bucket_counts(samples: List[int]) -> Dict[str, int]:
    """Generate cumulative histogram bucket counts for the default buckets."""
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


@dataclass
class LatencyProfile:
    """Percentile-based latency description in milliseconds."""

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
    """Traffic generation parameters for a sink."""

    records_per_second: float = 500.0
    burst_records: int = 0
    error_rate: float = 0.0
    latency: LatencyProfile = field(default_factory=LatencyProfile)

    @classmethod
    def from_dict(cls, defaults: Dict[str, Any], override: Optional[Dict[str, Any]]) -> "GenerationProfile":
        base = dict(defaults or {})
        override = override or {}
        latency_defaults = base.get("latencyMs", {})
        latency_override = override.get("latencyMs", {})
        latency = LatencyProfile.from_dict({**latency_defaults, **latency_override})
        return cls(
            records_per_second=float(
                override.get("recordsPerSecond", base.get("recordsPerSecond", 500.0))
            ),
            burst_records=int(override.get("burstRecords", base.get("burstRecords", 0))),
            error_rate=float(override.get("errorRate", base.get("errorRate", 0.0))),
            latency=latency,
        )


@dataclass
class CollectionTarget:
    """MongoDB collection target with optional label overrides."""

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
            return cls(
                name=name,
                kind=kind,
                label_database=label_database,
                label_collection=label_collection,
            )
        raise TypeError("Collection entries must be strings or mappings with 'name'.")


@dataclass
class SinkConfig:
    """All configuration needed to materialise a Kafka → Mongo sink."""

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
            except TypeError as exc:  # pragma: no cover - defensive programming
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


__all__ = [
    "CollectionTarget",
    "DEFAULT_APP_NAME",
    "FAILURE_STAGES",
    "GenerationProfile",
    "HISTOGRAM_BUCKETS_MS",
    "INT64_MAX",
    "INT64_MIN",
    "LatencyProfile",
    "SinkConfig",
    "build_bucket_counts",
    "compute_quantile",
]
