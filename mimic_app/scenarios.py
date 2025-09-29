"""Scenario definitions used to generate realistic metric series."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Mapping, MutableMapping

from .models import FAILURE_STAGES, GenerationProfile, LatencyProfile


@dataclass(frozen=True)
class ScenarioSample:
    """Materialised traffic sample for a single simulation run."""

    total_records: int
    successes: int
    errors: int
    failure_breakdown: Dict[str, int]
    duplicates: int
    processor_latencies_ms: List[int]
    mongo_latencies_ms: List[int]
    failure_latencies_ms: List[int]
    run_latency_ms: int

    @property
    def destination_records(self) -> int:
        return self.successes

    @property
    def global_records(self) -> int:
        return self.total_records

    @property
    def global_only_records(self) -> int:
        return self.duplicates


@dataclass(frozen=True)
class ScenarioDefinition:
    """User-selectable scenario describing error rates and latency envelopes."""

    key: str
    label: str
    description: str
    generation: GenerationProfile
    failure_bias: Mapping[str, float]
    mongo_slowdown: float = 1.0
    run_latency_scale: float = 1.0
    consumer_error_probability: float = 0.05
    kafka_error_probability: float = 0.02
    failure_spike_chance: float = 0.0
    failure_spike_multiplier: float = 0.0
    max_latency_samples: int = 60

    def sample(self, records: int, rng) -> ScenarioSample:
        """Generate counters and latency samples for the requested record count."""
        records = max(int(records), 0)
        if records == 0:
            empty_breakdown = {stage: 0 for stage in FAILURE_STAGES}
            return ScenarioSample(
                total_records=0,
                successes=0,
                errors=0,
                failure_breakdown=empty_breakdown,
                duplicates=0,
                processor_latencies_ms=[],
                mongo_latencies_ms=[],
                failure_latencies_ms=[],
                run_latency_ms=int(self.generation.latency.p50),
            )

        mean_errors = records * max(self.generation.error_rate, 0.0)
        stddev = max(mean_errors * 0.35, 1.0)
        errors = int(round(rng.gauss(mean_errors, stddev)))
        if self.failure_spike_chance and rng.random() < self.failure_spike_chance:
            errors += int(records * self.failure_spike_multiplier)
        errors = max(0, min(errors, records))
        successes = records - errors

        sample_count = min(max(successes, 1), self.max_latency_samples)
        processor_latencies = [self._sample_latency_ms(rng) for _ in range(sample_count)]
        mongo_latencies = [int(round(value * self.mongo_slowdown)) for value in processor_latencies]

        failure_latency_count = min(max(errors, 1), max(5, self.max_latency_samples // 2))
        failure_latencies = [
            int(round(self._sample_latency_ms(rng) * self.mongo_slowdown * 1.35))
            for _ in range(failure_latency_count)
        ] if errors else []

        run_latency_ms = int(round(self._sample_latency_ms(rng) * self.run_latency_scale))

        breakdown = self._build_failure_breakdown(errors, rng)
        duplicates = breakdown.get("duplicate_message", 0)
        return ScenarioSample(
            total_records=records,
            successes=successes,
            errors=errors,
            failure_breakdown=breakdown,
            duplicates=duplicates,
            processor_latencies_ms=processor_latencies,
            mongo_latencies_ms=mongo_latencies,
            failure_latencies_ms=failure_latencies,
            run_latency_ms=run_latency_ms,
        )

    def _sample_latency_ms(self, rng) -> float:
        profile = self.generation.latency
        roll = rng.random()
        if roll < 0.5:
            return max(rng.uniform(profile.p50 * 0.5, profile.p50 * 1.2), 1.0)
        if roll < 0.9:
            return max(rng.uniform(profile.p50, profile.p95), 1.0)
        if roll < 0.99:
            return max(rng.uniform(profile.p95, profile.p99), 1.0)
        return max(rng.uniform(profile.p99, profile.p99 * 1.6), 1.0)

    def _build_failure_breakdown(self, errors: int, rng) -> Dict[str, int]:
        breakdown: MutableMapping[str, int] = {stage: 0 for stage in FAILURE_STAGES}
        if errors <= 0:
            return dict(breakdown)

        weights: List[float] = [float(self.failure_bias.get(stage, 1.0)) for stage in FAILURE_STAGES]
        total_weight = sum(weights) or len(weights)
        remaining = errors
        for idx, stage in enumerate(FAILURE_STAGES):
            if remaining <= 0:
                break
            if idx == len(FAILURE_STAGES) - 1:
                count = remaining
            else:
                share = weights[idx] / total_weight
                expected = errors * share
                jitter = rng.uniform(-0.2, 0.2) * expected
                count = max(0, min(remaining, int(round(expected + jitter))))
            breakdown[stage] = breakdown.get(stage, 0) + count
            remaining -= count
        if remaining > 0:
            breakdown[FAILURE_STAGES[-1]] = breakdown.get(FAILURE_STAGES[-1], 0) + remaining
        return dict(breakdown)


def default_scenarios() -> Dict[str, ScenarioDefinition]:
    """Return the built-in scenario catalogue."""
    return {
        "payments_straight_through": ScenarioDefinition(
            key="payments_straight_through",
            label="Payments straight-through",
            description="Healthy payment capture with light duplicate suppression and minimal retries.",
            generation=GenerationProfile(
                records_per_second=540,
                error_rate=0.022,
                latency=LatencyProfile(p50=48, p95=128, p99=280),
            ),
            failure_bias={
                "json_structure_validation": 0.6,
                "retrieve_from_schema_registry": 0.8,
                "transformation": 1.1,
                "duplicate_message": 0.7,
                "mongo_pre_validation": 0.5,
                "deserialization": 0.4,
                "unknown": 0.3,
            },
            mongo_slowdown=1.08,
            run_latency_scale=1.15,
            consumer_error_probability=0.05,
            kafka_error_probability=0.015,
            max_latency_samples=48,
        ),
        "schema_validation_fallout": ScenarioDefinition(
            key="schema_validation_fallout",
            label="Schema validation fallout",
            description="Large spike of new schema versions causing validation rejects before saving.",
            generation=GenerationProfile(
                records_per_second=410,
                error_rate=0.44,
                latency=LatencyProfile(p50=140, p95=360, p99=840),
            ),
            failure_bias={
                "json_structure_validation": 5.2,
                "retrieve_from_schema_registry": 1.4,
                "transformation": 0.8,
                "duplicate_message": 0.3,
                "mongo_pre_validation": 0.6,
                "deserialization": 0.9,
                "unknown": 0.4,
            },
            mongo_slowdown=1.42,
            run_latency_scale=1.9,
            consumer_error_probability=0.28,
            kafka_error_probability=0.12,
            failure_spike_chance=0.5,
            failure_spike_multiplier=0.32,
            max_latency_samples=58,
        ),
        "duplicate_replay_window": ScenarioDefinition(
            key="duplicate_replay_window",
            label="Duplicate replay quarantine",
            description="Replay catch-up where duplicate detection routes most traffic away from destination collections.",
            generation=GenerationProfile(
                records_per_second=360,
                error_rate=0.27,
                latency=LatencyProfile(p50=105, p95=310, p99=680),
            ),
            failure_bias={
                "json_structure_validation": 0.5,
                "retrieve_from_schema_registry": 0.7,
                "transformation": 0.9,
                "duplicate_message": 4.6,
                "mongo_pre_validation": 0.6,
                "deserialization": 0.5,
                "unknown": 0.8,
            },
            mongo_slowdown=1.22,
            run_latency_scale=1.5,
            consumer_error_probability=0.18,
            kafka_error_probability=0.07,
            failure_spike_chance=0.42,
            failure_spike_multiplier=0.2,
            max_latency_samples=52,
        ),
        "destination_validation_backlog": ScenarioDefinition(
            key="destination_validation_backlog",
            label="Destination validation backlog",
            description="Destination collection applies stricter validation, creating mongo pre-validation rejects and longer commits.",
            generation=GenerationProfile(
                records_per_second=620,
                error_rate=0.16,
                latency=LatencyProfile(p50=92, p95=255, p99=540),
            ),
            failure_bias={
                "json_structure_validation": 1.2,
                "retrieve_from_schema_registry": 1.0,
                "transformation": 2.1,
                "duplicate_message": 0.9,
                "mongo_pre_validation": 3.8,
                "deserialization": 0.6,
                "unknown": 0.7,
            },
            mongo_slowdown=1.28,
            run_latency_scale=1.4,
            consumer_error_probability=0.14,
            kafka_error_probability=0.06,
            failure_spike_chance=0.3,
            failure_spike_multiplier=0.18,
            max_latency_samples=56,
        ),
    }


__all__ = ["ScenarioDefinition", "ScenarioSample", "default_scenarios"]
