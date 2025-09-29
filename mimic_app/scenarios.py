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
    processor_latencies_ms: List[int]
    mongo_latencies_ms: List[int]
    failure_latencies_ms: List[int]
    run_latency_ms: int


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
        return ScenarioSample(
            total_records=records,
            successes=successes,
            errors=errors,
            failure_breakdown=breakdown,
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
        "steady_state": ScenarioDefinition(
            key="steady_state",
            label="Steady state",
            description="Nominal traffic with occasional transformation retries.",
            generation=GenerationProfile(
                records_per_second=600,
                error_rate=0.015,
                latency=LatencyProfile(p50=55, p95=140, p99=320),
            ),
            failure_bias={
                "json_structure_validation": 0.5,
                "retrieve_from_schema_registry": 0.8,
                "transformation": 1.4,
                "deserialization": 0.6,
                "unknown": 0.3,
            },
            mongo_slowdown=1.05,
            run_latency_scale=1.1,
            consumer_error_probability=0.04,
            kafka_error_probability=0.01,
            max_latency_samples=50,
        ),
        "failure_spike": ScenarioDefinition(
            key="failure_spike",
            label="Schema registry outage",
            description="High failure rate concentrated on schema registry lookups with knock-on effects.",
            generation=GenerationProfile(
                records_per_second=450,
                error_rate=0.55,
                latency=LatencyProfile(p50=140, p95=420, p99=900),
            ),
            failure_bias={
                "json_structure_validation": 0.4,
                "retrieve_from_schema_registry": 4.0,
                "transformation": 1.0,
                "deserialization": 0.8,
                "unknown": 1.2,
            },
            mongo_slowdown=1.35,
            run_latency_scale=1.7,
            consumer_error_probability=0.35,
            kafka_error_probability=0.18,
            failure_spike_chance=0.65,
            failure_spike_multiplier=0.25,
            max_latency_samples=60,
        ),
        "edge_case": ScenarioDefinition(
            key="edge_case",
            label="Edge case replay",
            description="Low-volume replay hammering validation and transformation edge cases.",
            generation=GenerationProfile(
                records_per_second=120,
                error_rate=0.35,
                latency=LatencyProfile(p50=220, p95=540, p99=1200),
            ),
            failure_bias={
                "json_structure_validation": 3.0,
                "retrieve_from_schema_registry": 0.9,
                "transformation": 2.4,
                "deserialization": 0.8,
                "unknown": 0.6,
            },
            mongo_slowdown=1.6,
            run_latency_scale=2.0,
            consumer_error_probability=0.12,
            kafka_error_probability=0.05,
            failure_spike_chance=0.35,
            failure_spike_multiplier=0.15,
            max_latency_samples=40,
        ),
        "recovery_burst": ScenarioDefinition(
            key="recovery_burst",
            label="Backlog recovery burst",
            description="Large backlog drain with a small tail of DLQ routing and higher latencies.",
            generation=GenerationProfile(
                records_per_second=900,
                error_rate=0.08,
                latency=LatencyProfile(p50=95, p95=260, p99=520),
            ),
            failure_bias={
                "json_structure_validation": 0.8,
                "retrieve_from_schema_registry": 0.9,
                "transformation": 1.8,
                "deserialization": 0.7,
                "unknown": 0.6,
            },
            mongo_slowdown=1.18,
            run_latency_scale=1.35,
            consumer_error_probability=0.1,
            kafka_error_probability=0.05,
            failure_spike_chance=0.25,
            failure_spike_multiplier=0.12,
            max_latency_samples=55,
        ),
    }


__all__ = ["ScenarioDefinition", "ScenarioSample", "default_scenarios"]
