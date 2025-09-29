"""HTTP entry point exposing the Alfred mimic control plane UI."""
from __future__ import annotations

import argparse
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional

import yaml
from flask import (
    Flask,
    Response,
    jsonify,
    render_template,
    request,
)

from .models import SinkConfig
from .service import MimicService, SimulationOutcome


@dataclass
class FormState:
    env: str
    topic: str
    collection: str
    collection_kind: str
    scenario: str
    records: int
    error: Optional[str] = None


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


def _find_collection_kind(service: MimicService, name: str, default_kind: str) -> str:
    for option in service.defaults["collections"]:
        if option.name == name:
            return option.kind
    return default_kind


def _create_form_state(service: MimicService, values: Dict[str, str]) -> FormState:
    defaults = service.defaults
    env = values.get("env") or (defaults["envs"][0] if defaults["envs"] else "dev")
    topic = values.get("topic") or (defaults["topics"][0] if defaults["topics"] else "topic")
    collection = values.get("collection") or defaults["collections"][0].name
    kind_default = defaults["collections"][0].kind if defaults["collections"] else "scalar"
    collection_kind = values.get("collection_kind") or _find_collection_kind(service, collection, kind_default)
    scenario = values.get("scenario") or next(iter(service.scenarios.keys()))
    records_raw = values.get("records") or "1000"
    try:
        records = max(int(records_raw), 0)
    except ValueError:
        records = 0
    return FormState(
        env=env.strip(),
        topic=topic.strip(),
        collection=collection.strip(),
        collection_kind=collection_kind.strip() or "scalar",
        scenario=scenario.strip(),
        records=records,
    )


def create_app(service: MimicService) -> Flask:
    package_root = Path(__file__).resolve().parent
    app = Flask(
        __name__,
        template_folder=str(package_root / "templates"),
        static_folder=str(package_root / "static"),
    )

    def _render(form: Optional[FormState] = None, result: Optional[SimulationOutcome] = None, error: Optional[str] = None):
        form = form or _create_form_state(service, {})
        status = service.get_collection_status()
        return render_template(
            "index.html",
            form=form,
            result=result or service.last_result,
            status=status,
            scenarios=service.scenarios,
            defaults=service.defaults,
            error=error or form.error,
        )

    @app.route("/", methods=["GET"])
    def index() -> str:
        form = _create_form_state(service, {})
        return _render(form=form)

    @app.route("/simulate", methods=["POST"])
    def simulate() -> str:
        form = _create_form_state(service, request.form.to_dict())
        error: Optional[str] = None
        if not form.env:
            error = "Environment is required."
        elif not form.topic:
            error = "Topic is required."
        elif not form.collection:
            error = "Collection name is required."
        elif form.collection_kind not in {"scalar", "summary"}:
            error = "Collection type must be 'scalar' or 'summary'."
        elif form.records <= 0:
            error = "Record count must be a positive integer."

        if error:
            form.error = error
            return _render(form=form, error=error)

        form.collection_kind = _find_collection_kind(service, form.collection, form.collection_kind)
        try:
            result = service.simulate(
                env=form.env,
                topic=form.topic,
                collection_name=form.collection,
                collection_kind=form.collection_kind,
                scenario_key=form.scenario,
                records=form.records,
            )
        except Exception as exc:  # pylint: disable=broad-except
            error = f"Failed to run simulation: {exc}"
            form.error = error
            return _render(form=form, error=error)
        return _render(form=form, result=result)

    @app.route("/api/simulate", methods=["POST"])
    def api_simulate() -> Response:
        payload = request.get_json(force=True, silent=True) or {}
        form = _create_form_state(service, {k: str(v) for k, v in payload.items()})
        error: Optional[str] = None
        if not form.env:
            error = "env is required"
        elif not form.topic:
            error = "topic is required"
        elif not form.collection:
            error = "collection is required"
        elif form.collection_kind not in {"scalar", "summary"}:
            error = "collection_kind must be 'scalar' or 'summary'"
        elif form.records <= 0:
            error = "records must be positive"

        if error:
            return jsonify({"error": error}), 400

        form.collection_kind = _find_collection_kind(service, form.collection, form.collection_kind)
        result = service.simulate(
            env=form.env,
            topic=form.topic,
            collection_name=form.collection,
            collection_kind=form.collection_kind,
            scenario_key=form.scenario,
            records=form.records,
        )
        return jsonify(
            {
                "timestamp": result.timestamp.isoformat(),
                "env": result.env,
                "topic": result.topic,
                "collection": result.collection,
                "kind": result.kind,
                "scenario": result.scenario.key,
                "records": result.sample.total_records,
                "successes": result.sample.successes,
                "errors": result.sample.errors,
                "failureBreakdown": result.sample.failure_breakdown,
                "runLatencyMs": result.sample.run_latency_ms,
            }
        )

    @app.route("/api/status", methods=["GET"])
    def api_status() -> Response:
        statuses = [status.as_dict() for status in service.get_collection_status()]
        return jsonify({"collections": statuses})

    @app.route("/metrics", methods=["GET"])
    def metrics() -> Response:
        payload, content_type = service.generate_metrics_response()
        return Response(payload, content_type=content_type)

    @app.teardown_appcontext
    def _teardown(exception: Optional[BaseException]) -> None:  # pylint: disable=unused-argument
        # Ensure Mongo clients are closed when the application shuts down.
        service.close()

    return app


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Alfred mimic control plane")
    parser.add_argument(
        "--config",
        default=os.environ.get("MIMIC_CONFIG", "config/mimic-topology.yml"),
        help="Path to the topology configuration file.",
    )
    parser.add_argument(
        "--mongo-uri",
        default=os.environ.get("MIMIC_MONGO_URI", "mongodb://localhost:27017"),
        help="Fallback MongoDB URI for ad-hoc simulations.",
    )
    parser.add_argument("--host", default=os.environ.get("MIMIC_HOST", "0.0.0.0"))
    parser.add_argument("--port", type=int, default=int(os.environ.get("MIMIC_PORT", "8000")))
    parser.add_argument("--debug", action="store_true", help="Run Flask in debug mode.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    configs = load_configuration(args.config)
    service = MimicService(configs, default_mongo_uri=args.mongo_uri)
    app = create_app(service)
    app.run(host=args.host, port=args.port, debug=args.debug)


if __name__ == "__main__":
    main()
