from __future__ import annotations

import json
from typing import Any

from opentelemetry.trace import Status, StatusCode

from dbt_events_consumer.runner import DbtRunnerWrapper
from worker.observability import get_tracer
from worker.outcomes import (
    HandlerOutcome,
    PermanentFailure,
    Success,
    TransientFailure,
)
from worker.schema import TriggerMessage

ROUTING_TO_SELECT: dict[str, str] = {
    "verified.rni": "stg_rni_combined+",
    "verified.iri": "stg_rni_combined tag:iri",
    "verified.pci": "stg_rni_combined tag:pci",
}


class DbtMessageHandler:
    """
    Domain handler for dbt model runs.

    Translates incoming ``TriggerMessage`` objects into dbt CLI invocations
    and maps the ``dbtRunnerResult`` to framework outcomes.
    """

    def __init__(
        self,
        settings: Any,
        runner: DbtRunnerWrapper,
    ) -> None:
        self._settings = settings
        self._runner = runner
        self._tracer = get_tracer("dbt_events_consumer")

    def handle(
        self,
        msg: TriggerMessage,
        routing_key: str,
        connection: Any,
    ) -> HandlerOutcome:
        selection = ROUTING_TO_SELECT[routing_key]

        with self._tracer.start_as_current_span("dbt_run") as run_span:
            run_span.set_attribute("dbt.selection", selection)
            run_span.set_attribute("dbt.year", msg.year)
            run_span.set_attribute("dbt.semester", msg.semester)

            try:
                result = self._runner.run(
                    self._build_cli_args(selection, msg),
                    connection=connection,
                )
            except Exception as exc:
                run_span.record_exception(exc)
                run_span.set_status(Status(StatusCode.ERROR, str(exc)))
                return TransientFailure(exc)

            outcome = self._interpret_result(result, selection, run_span)
            if isinstance(outcome, TransientFailure):
                run_span.record_exception(outcome.exc)
                run_span.set_status(Status(StatusCode.ERROR, str(outcome.exc)))
            elif isinstance(outcome, PermanentFailure):
                run_span.set_status(
                    Status(StatusCode.ERROR, outcome.reason),
                )
            return outcome

    def _interpret_result(
        self,
        result: Any,
        selection: str,
        run_span: Any,
    ) -> HandlerOutcome:
        node_results = result.result or []
        node_count = len(node_results)

        if result.success and node_count == 0:
            return PermanentFailure(
                reason="empty_selection",
                detail=f"selection {selection!r} produced 0 models",
                metrics={"node_count": 0},
            )

        if not result.success and result.exception is None:
            statuses = self._collect_node_statuses(node_results)
            return PermanentFailure(
                reason="dbt_run_failed",
                detail=f"dbt run failed; node statuses: {statuses}",
                metrics={"node_count": node_count},
            )

        if result.exception is not None:
            return TransientFailure(result.exception)

        run_span.set_attribute("dbt.node_count", node_count)
        return Success(metrics={"node_count": node_count})

    def _build_cli_args(
        self, selection: str, msg: TriggerMessage
    ) -> list[str]:
        vars_dict: dict[str, Any] = {
            "year": msg.year,
            "semester": msg.semester,
        }
        if msg.routes:
            vars_dict["routes"] = msg.routes

        cli_args = [
            "run",
            "--select",
            selection,
            "--vars",
            json.dumps(vars_dict),
        ]
        if msg.full_refresh:
            cli_args.append("--full-refresh")
        return cli_args

    def _collect_node_statuses(self, node_results: list[Any]) -> dict[str, str]:
        statuses: dict[str, str] = {}
        for node in node_results:
            uid = getattr(node, "unique_id", str(node))
            status = getattr(node, "status", "unknown")
            statuses[uid] = str(status)
        return statuses
