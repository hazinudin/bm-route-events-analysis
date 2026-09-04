from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from dbt_events_consumer.schema import TriggerMessage


def _valid_payload(**overrides):
    payload = {
        "job_id": "b7e6c1c4-7f5b-4c96-9b34-5d4f5f4b3d12",
        "routing_key": "verified.rni",
        "year": 2025,
        "semester": 2,
        "emitted_at": "2026-08-21T09:30:00Z",
    }
    payload.update(overrides)
    return payload


def test_valid_rni_payload():
    msg = TriggerMessage(**_valid_payload())
    assert msg.job_id == "b7e6c1c4-7f5b-4c96-9b34-5d4f5f4b3d12"
    assert msg.routing_key == "verified.rni"
    assert msg.year == 2025
    assert msg.semester == 2
    assert msg.routes is None
    assert msg.full_refresh is False
    assert msg.emitted_at == datetime(2026, 8, 21, 9, 30, tzinfo=timezone.utc)


def test_valid_iri_payload_routes_none():
    msg = TriggerMessage(**_valid_payload(routing_key="verified.iri", semester=1))
    assert msg.routing_key == "verified.iri"
    assert msg.routes is None


def test_valid_pci_payload_with_routes():
    msg = TriggerMessage(
        **_valid_payload(
            routing_key="verified.pci",
            routes=["01001", "01002"],
            full_refresh=True,
        )
    )
    assert msg.routing_key == "verified.pci"
    assert msg.routes == ["01001", "01002"]
    assert msg.full_refresh is True


def test_full_refresh_defaults_false():
    msg = TriggerMessage(**_valid_payload())
    assert msg.full_refresh is False


def test_missing_semester_raises():
    payload = _valid_payload()
    del payload["semester"]
    with pytest.raises(ValidationError):
        TriggerMessage(**payload)


def test_missing_job_id_raises():
    payload = _valid_payload()
    del payload["job_id"]
    with pytest.raises(ValidationError):
        TriggerMessage(**payload)


def test_semester_out_of_range_raises():
    with pytest.raises(ValidationError):
        TriggerMessage(**_valid_payload(semester=3))


def test_semester_zero_raises():
    with pytest.raises(ValidationError):
        TriggerMessage(**_valid_payload(semester=0))


def test_semester_one_valid():
    msg = TriggerMessage(**_valid_payload(semester=1))
    assert msg.semester == 1


def test_emitted_at_parsed_iso8601():
    msg = TriggerMessage(**_valid_payload(emitted_at="2026-08-21T09:30:00+00:00"))
    assert msg.emitted_at.tzinfo is not None
