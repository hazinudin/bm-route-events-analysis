from __future__ import annotations

from typing import Any

import oracledb

from traffic.consumer.config import TrafficWorkerSettings
from traffic.vcr_aadt_adt.pipeline import AADTPipeline
from traffic.vcr_aadt_adt.writer import write_aadt_results
from worker.handler import MessageHandler
from worker.heartbeat import run_with_heartbeat
from worker.outcomes import (
    HandlerOutcome,
    PermanentFailure,
    Success,
    TransientFailure,
)
from worker.schema import TriggerMessage


class TrafficMessageHandler(MessageHandler):
    """
    Domain handler for traffic AADT/VCR calculation events.

    Listens exclusively for ``verified.rtc`` routing key, runs the
    ``AADTPipeline`` for the requested routes/year/semester, and persists
    the result DataFrame to the configured Oracle target table.
    """

    def __init__(self, settings: TrafficWorkerSettings) -> None:
        self._settings = settings

    def handle(
        self,
        msg: TriggerMessage,
        routing_key: str,
        connection: Any,
    ) -> HandlerOutcome:
        try:
            oracle_conn = self._connect_oracle()
        except Exception as exc:
            return TransientFailure(exc)

        try:
            df = run_with_heartbeat(
                lambda: AADTPipeline(connection=oracle_conn).calculate(
                    routes=msg.routes,
                    year=msg.year,
                    semester=msg.semester,
                ),
                connection,
            )
            row_count = write_aadt_results(
                connection=oracle_conn,
                df=df,
                year=msg.year,
                semester=msg.semester,
                routes=msg.routes,
                target_table=self._settings.traffic_target_table,
            )
            return Success(metrics={"row_count": row_count})
        except oracledb.DatabaseError as exc:
            return TransientFailure(exc)
        except Exception as exc:
            return PermanentFailure(
                reason="traffic_calculation_failed",
                detail=str(exc),
                metrics={"error_type": type(exc).__name__},
            )
        finally:
            try:
                oracle_conn.close()
            except Exception:
                pass

    def _connect_oracle(self) -> oracledb.Connection:
        dsn = oracledb.makedsn(
            self._settings.oracle_host,
            self._settings.oracle_port,
            service_name=self._settings.oracle_service,
        )
        return oracledb.connect(
            user=self._settings.oracle_user,
            password=self._settings.oracle_password,
            dsn=dsn,
        )
