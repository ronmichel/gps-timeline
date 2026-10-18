"""The Google import must remain readable by the unmodified upstream store."""

import asyncio
from copy import deepcopy
from datetime import datetime
from io import StringIO
import json
from pathlib import Path
import sqlite3
import threading

from homeassistant.exceptions import HomeAssistantError
from homeassistant.helpers import entity_registry as er
import pytest
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.gps_timeline import services
from custom_components.gps_timeline.const import (
    CONF_ACTIVITY_ENTITY,
    CONF_ENTITY_ID,
    CONF_PLACES_ENTITY,
    DB_DIR_NAME,
    DB_FILE_NAME,
    DOMAIN,
)
from tests.test_services import make_state

ENTITY = "device_tracker.phone"
PLACES = "sensor.places_phone"
ACTIVITY = "sensor.activity_phone"


def timestamp(hour, minute=0):
    return f"2020-01-02T{hour:02}:{minute:02}:00+02:00"


def visit(start=10, end=11, **extra):
    return {
        "startTime": timestamp(start),
        "endTime": timestamp(end),
        "visit": {
            "hierarchyLevel": 0,
            "topCandidate": {
                "semanticType": "WORK",
                "placeId": "synthetic-place-id",
                "probability": 0.95,
                "placeLocation": {"latLng": "51.0°, 9.0°"},
            },
            **extra,
        },
    }


def activity(start, end, kind="WALKING"):
    return {
        "startTime": start,
        "endTime": end,
        "activity": {
            "topCandidate": {"type": kind, "probability": 0.8},
            "start": {"latLng": "52.0°, 10.0°"},
            "end": {"latLng": "53.0°, 11.0°"},
            "distanceMeters": 2000,
        },
    }


def export():
    return {
        "semanticSegments": [
            visit(),
            activity(timestamp(11), timestamp(11, 30), "IN_VEHICLE"),
            activity(timestamp(11, 30), timestamp(12)),
            {
                "startTime": timestamp(10),
                "endTime": timestamp(12),
                "timelinePath": [
                    {"point": "50°, 8°", "time": timestamp(10)},
                    {"point": "50.12345678°, 8°", "time": timestamp(11)},
                    {"point": "50.2°, 8°", "time": timestamp(11)},
                    {"point": "geo:50.3,8.3", "durationMinutesOffsetFromStartTime": "61"},
                ],
            },
        ],
        "rawSignals": [{"ignored": 'braces { and escaped quotes " are not delimiters'}],
    }


async def setup_import(hass, payload=None, *, companions=True):
    data = {CONF_ENTITY_ID: ENTITY}
    if companions:
        data.update({CONF_PLACES_ENTITY: PLACES, CONF_ACTIVITY_ENTITY: ACTIVITY})
    entry = MockConfigEntry(domain=DOMAIN, data=data, title="Phone")
    entry.add_to_hass(hass)
    assert await hass.config_entries.async_setup(entry.entry_id)
    await hass.async_block_till_done()
    folder = Path(hass.config.path(DB_DIR_NAME))
    (folder / "Timeline.json").write_text(json.dumps(payload or export()), encoding="utf-8")
    return entry, folder / DB_FILE_NAME


async def call_import(hass, **overrides):
    return await hass.services.async_call(
        DOMAIN,
        services.GOOGLE_IMPORT_SERVICE,
        {CONF_ENTITY_ID: ENTITY, "file_name": "Timeline.json", **overrides},
        blocking=True,
        return_response=True,
    )


def archive_snapshot(path):
    with sqlite3.connect(path) as conn:
        return {
            "schema": conn.execute(
                "SELECT name, sql FROM sqlite_master ORDER BY name"
            ).fetchall(),
            "version": conn.execute("PRAGMA user_version").fetchone()[0],
            "points": conn.execute("SELECT * FROM points ORDER BY ts").fetchall(),
            "states": conn.execute(
                "SELECT * FROM entity_states ORDER BY entity_id, ts"
            ).fetchall(),
        }


async def test_import_preserves_upstream_schema_entities_existing_rows_and_is_idempotent(hass):
    entry, path = await setup_import(hass)
    store = hass.data[DOMAIN]["store"]
    tracker_id = (await store.async_get_tracker_by_entity(ENTITY))["tracker_id"]
    original = make_state(
        ENTITY,
        "home",
        {"latitude": 60.0, "longitude": 5.0},
        datetime.fromisoformat(timestamp(10)).timestamp(),
    )
    await store.async_write_backfill([(tracker_id, services.normalize_point(original))], [])
    before = archive_snapshot(path)
    entities = set(er.async_get(hass).entities)
    states = set(hass.states.async_entity_ids())
    config = dict(entry.data)

    result = await call_import(hass)
    assert result["points_read"] == 10
    assert result["points_inserted"] == 4
    assert result["points_skipped"] == 6
    assert result["visits_read"] == 1
    assert result["activities_read"] == 2
    assert result["visit_states_inserted"] == 2
    assert result["activity_states_inserted"] == 3
    assert result["days"] == 1
    after = archive_snapshot(path)
    assert after["schema"] == before["schema"]
    assert after["version"] == 2
    assert len(after["points"]) == 5
    assert after["points"][0] == before["points"][0]
    assert set(er.async_get(hass).entities) == entities
    assert set(hass.states.async_entity_ids()) == states
    assert dict(entry.data) == config

    # The original Store/WebSocket row format is sufficient; no custom query code.
    history = await store.async_query_states([ENTITY, PLACES, ACTIVITY], 0, 2_000_000_000)
    assert history[ENTITY][1]["a"]["latitude"] == 50.123457
    assert history[ENTITY][1]["lu"] == datetime.fromisoformat(timestamp(11)).timestamp()
    assert [row["s"] for row in history[PLACES]] == ["Work", "unknown"]
    assert [row["s"] for row in history[ACTIVITY]] == ["automotive", "walking", "unknown"]
    assert history[PLACES][0]["a"]["google_timeline"]["visit"] == visit()["visit"]
    assert history[ACTIVITY][0]["a"]["google_timeline"]["activity"]["distanceMeters"] == 2000
    again = await call_import(hass)
    assert again["days"] == again["points_inserted"] == 0
    assert again["visit_states_inserted"] == again["activity_states_inserted"] == 0
    assert archive_snapshot(path) == after


async def test_overlap_and_adjacent_intervals_use_normal_state_boundaries(hass):
    parent = visit(start=9, end=12)
    nested = visit(start=10, end=11, hierarchyLevel=1)
    nested["visit"]["topCandidate"]["placeName"] = "Nested shop"
    payload = {
        "semanticSegments": [
            nested,
            parent,
            parent,  # Duplicated and out of order.
            activity(timestamp(9), timestamp(12), "IN_VEHICLE"),
            activity(timestamp(10), timestamp(11)),
            activity(timestamp(12), timestamp(13), "CYCLING"),
        ]
    }
    await setup_import(hass, payload)
    await call_import(hass)
    history = await hass.data[DOMAIN]["store"].async_query_states(
        [PLACES, ACTIVITY], 0, 2_000_000_000
    )
    assert [row["s"] for row in history[PLACES]] == ["Work", "Work", "Work", "unknown"]
    overlap = history[PLACES][1]["a"]["google_overlapping_intervals"][0]
    assert overlap["google_timeline"]["visit"]["topCandidate"]["placeName"] == "Nested shop"
    assert [row["s"] for row in history[ACTIVITY]] == [
        "automotive",
        "walking",
        "automotive",
        "cycling",
        "unknown",
    ]


async def test_targets_can_be_supplied_without_creating_sensors(hass):
    await setup_import(hass, companions=False)
    with pytest.raises(HomeAssistantError, match="places_entity"):
        await call_import(hass)
    result = await call_import(hass, places_entity=PLACES, activity_entity=ACTIVITY)
    assert result["visit_states_inserted"] == 2
    assert result["activity_states_inserted"] == 3
    assert hass.states.get(PLACES) is hass.states.get(ACTIVITY) is None
    assert er.async_get(hass).async_get(PLACES) is None
    assert er.async_get(hass).async_get(ACTIVITY) is None


async def test_path_only_export_needs_no_companions(hass):
    await setup_import(
        hass, {"semanticSegments": [export()["semanticSegments"][-1]]}, companions=False
    )
    result = await call_import(hass)
    assert result["points_inserted"] == 3
    assert result["visit_states_inserted"] == result["activity_states_inserted"] == 0


@pytest.mark.parametrize("damage", ["truncated", "trailing", "coordinates", "time", "nan"])
async def test_invalid_file_does_not_partially_import(hass, damage):
    _, path = await setup_import(hass)
    payload = deepcopy(export())
    if damage == "coordinates":
        payload["semanticSegments"][-1]["timelinePath"][-1]["point"] = "91°, 8°"
    if damage == "time":
        payload["semanticSegments"][-1]["timelinePath"][-1]["time"] = "2020-01-02T11:00:00"
    text = json.dumps(payload)
    if damage == "truncated":
        text = text[:-1]
    elif damage == "trailing":
        text += " false"
    elif damage == "nan":
        text = text[:-1] + ', "invalid": NaN}'
    path.with_name("Timeline.json").write_text(text)
    before = archive_snapshot(path)
    with pytest.raises(HomeAssistantError):
        await call_import(hass)
    assert archive_snapshot(path) == before


@pytest.mark.parametrize(
    "name", ["../Timeline.json", "/tmp/Timeline.json", "dir\\Timeline.json", "gps_timeline.db"]
)
async def test_only_json_basename_in_database_directory_is_accepted(hass, name):
    await setup_import(hass)
    with pytest.raises(HomeAssistantError, match="filename"):
        await call_import(hass, file_name=name)


async def test_symlink_outside_database_directory_is_rejected(hass, tmp_path):
    _, path = await setup_import(hass)
    outside = tmp_path / "outside.json"
    outside.write_text(json.dumps(export()))
    path.with_name("link.json").symlink_to(outside)
    with pytest.raises(HomeAssistantError, match="same directory"):
        await call_import(hass, file_name="link.json")


@pytest.mark.parametrize("chunk_size", [1, 2, 3, 7, 64 * 1024])
def test_streaming_json_handles_boundaries_and_additional_top_level_fields(chunk_size):
    class Chunks(StringIO):
        def read(self, size):
            return super().read(min(size, chunk_size))

    payload = export()
    payload["rawSignals"] = [1.25, 2e-10, {"nested": ['é°\\"', {}]}]
    payload = {"before": [1e30], **payload, "after": {"object": True}}
    actual = list(services._GoogleJSONReader(Chunks(json.dumps(payload))).segments())
    assert actual == payload["semanticSegments"]


@pytest.mark.parametrize(
    "text",
    [
        '{"semanticSegments":[],}',
        '{"semanticSegments":[{},]}',
        '{"semanticSegments":[],"semanticSegments":[]}',
        '{"semanticSegments":[],"number":1e}',
        '{"somethingElse":[]}',
        '{"semanticSegments":{}}',
    ],
)
def test_streaming_reader_rejects_invalid_envelopes(text):
    with pytest.raises(ValueError):
        list(services._GoogleJSONReader(StringIO(text)).segments())


async def test_import_cannot_mix_history_from_different_trackers(hass):
    _, path = await setup_import(hass)
    with sqlite3.connect(path) as conn:
        other = conn.execute(
            "INSERT INTO trackers(entity_id, created_at) VALUES ('device_tracker.other', 0)"
        ).lastrowid
        conn.execute(
            "INSERT INTO entity_states(tracker_id, entity_id, ts, state)"
            " VALUES (?, ?, 0, 'x')",
            (other, PLACES),
        )
    before = archive_snapshot(path)
    with pytest.raises(HomeAssistantError, match="another tracker"):
        await call_import(hass)
    assert archive_snapshot(path) == before


async def test_sql_failure_rolls_back_all_new_archive_rows(hass, monkeypatch):
    _, path = await setup_import(hass)
    before = archive_snapshot(path)
    connect = sqlite3.connect

    class FailAfterPoints(sqlite3.Connection):
        def execute(self, sql, *args, **kwargs):
            result = super().execute(sql, *args, **kwargs)
            if sql.startswith("INSERT OR IGNORE INTO points"):
                raise sqlite3.OperationalError(
                    "simulated storage error after inserting points"
                )
            return result

    monkeypatch.setattr(
        services.sqlite3,
        "connect",
        lambda *a, **kw: connect(*a, **kw, factory=FailAfterPoints),
    )
    with pytest.raises(HomeAssistantError, match="simulated storage error"):
        await call_import(hass)
    assert archive_snapshot(path) == before


async def test_live_writes_continue_during_staging_and_take_precedence(hass, monkeypatch):
    _, path = await setup_import(hass)
    stage = services._stage_google_segment
    inserted = False

    def stage_with_live_write(*args):
        nonlocal inserted
        counts = stage(*args)
        if not inserted:
            with sqlite3.connect(path, timeout=0.1) as other:
                tracker = other.execute(
                    "SELECT id FROM trackers WHERE entity_id=?", (ENTITY,)
                ).fetchone()[0]
                other.execute(
                    "INSERT INTO points(tracker_id, ts, lat, lon, attributes)"
                    " VALUES (?, ?, 61, 6, '{}')",
                    (tracker, datetime.fromisoformat(timestamp(10)).timestamp()),
                )
            inserted = True
        return counts

    monkeypatch.setattr(services, "_stage_google_segment", stage_with_live_write)
    result = await call_import(hass)
    assert inserted
    assert result["points_inserted"] == 4
    assert archive_snapshot(path)["points"][0][4:6] == (61, 6)


async def test_cancellation_waits_for_worker_and_leaves_archive_unchanged(hass, monkeypatch):
    _, path = await setup_import(hass)
    before = archive_snapshot(path)
    entered, resume = threading.Event(), threading.Event()
    stage = services._stage_google_segment

    def pause(*args):
        counts = stage(*args)
        entered.set()
        assert resume.wait(10)
        return counts

    monkeypatch.setattr(services, "_stage_google_segment", pause)
    task = asyncio.create_task(call_import(hass))
    assert await hass.async_add_executor_job(entered.wait, 10)
    task.cancel()
    await asyncio.sleep(0)
    resume.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert archive_snapshot(path) == before
