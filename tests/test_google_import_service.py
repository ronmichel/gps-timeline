"""The Google import must remain readable by the unmodified upstream store."""

import asyncio
from copy import deepcopy
from datetime import datetime
import gzip
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
CAR = "device_tracker.car"


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


def parked_activity(time, coordinates="54.12345678°, 12.2°"):
    segment = activity(timestamp(10), timestamp(11), "IN_PASSENGER_VEHICLE")
    segment["activity"]["parking"] = {
        "startTime": time,
        "location": {"latLng": coordinates},
        "extra": {"retained": "whole parking object"},
    }
    return segment


async def setup_car(hass):
    entry = MockConfigEntry(domain=DOMAIN, data={CONF_ENTITY_ID: CAR}, title="Car")
    entry.add_to_hass(hass)
    assert await hass.config_entries.async_setup(entry.entry_id)
    await hass.async_block_till_done()
    return entry


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


def read_log(path):
    with gzip.open(path, "rt", encoding="utf-8") as stream:
        records = [json.loads(line) for line in stream]
    assert records[0]["event"] == "header"
    assert records[-1]["event"] == "summary"
    return records


def log_items(records, code):
    return [record for record in records if record.get("code") == code]


def only_log(db_path):
    logs = list(db_path.parent.glob("google_timeline_import_*.jsonl.gz"))
    assert len(logs) == 1
    return logs[0]


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


@pytest.mark.parametrize("damage", ["truncated", "trailing", "nan"])
async def test_invalid_file_does_not_partially_import(hass, damage):
    _, path = await setup_import(hass)
    payload = deepcopy(export())
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
    records = read_log(only_log(path))
    assert records[-1]["committed"] is False
    assert records[-1]["result"]["points_inserted"] == 0
    assert len(log_items(records, "import_aborted")) == 8


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


@pytest.mark.parametrize("with_car", [False, True])
async def test_cancellation_waits_for_worker_and_leaves_archive_unchanged(
    hass, monkeypatch, with_car
):
    payload = export()
    if with_car:
        payload["semanticSegments"][0] = parked_activity(timestamp(10))
    _, path = await setup_import(hass, payload)
    if with_car:
        await setup_car(hass)
    before = archive_snapshot(path)
    entered, resume = threading.Event(), threading.Event()
    stage = services._stage_google_segment

    def pause(*args):
        counts = stage(*args)
        entered.set()
        assert resume.wait(10)
        return counts

    monkeypatch.setattr(services, "_stage_google_segment", pause)
    task = asyncio.create_task(
        call_import(hass, **({"car_entity_id": CAR} if with_car else {}))
    )
    assert await hass.async_add_executor_job(entered.wait, 10)
    task.cancel()
    await asyncio.sleep(0)
    resume.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert archive_snapshot(path) == before
    records = read_log(only_log(path))
    assert records[-1]["status"] == "cancelled"
    assert records[-1]["committed"] is False
    assert log_items(records, "import_aborted")
    assert log_items(records, "unparsed_json_text")
    if with_car:
        assert any(
            item.get("entity_id") == CAR for item in log_items(records, "import_aborted")
        )


async def test_every_call_has_a_private_complete_log_including_existing_rows(hass):
    _, path = await setup_import(hass)
    first = await call_import(hass)
    second = await call_import(hass)
    assert first["log_file"] != second["log_file"]
    assert len(list(path.parent.glob("google_timeline_import_*.jsonl.gz"))) == 2
    for result in (first, second):
        logfile = Path(result["log_file"])
        assert logfile.parent == path.parent
        assert logfile.stat().st_mode & 0o777 == 0o600
        summary = read_log(logfile)[-1]
        assert summary["status"] == "completed"
        assert summary["committed"] is summary["parse_complete"] is True
        assert summary["result"] == result
    records = read_log(second["log_file"])
    assert len(log_items(records, "existing_point")) == 5
    states = log_items(records, "existing_companion_state")
    assert len(states) == 5
    assert states[0]["source_items"]
    visit_sources = [
        source
        for record in states
        for source in record["source_items"]
        if source["path"] == "$.semanticSegments[0].visit"
    ]
    assert visit_sources[0]["data"] == visit()["visit"]


async def test_whole_json_is_inventoried_and_unknown_values_are_not_truncated(hass):
    payload = {
        "before": [{"nested": [1, None, True, {"name": "ü" * 70000}]}],
        **export(),
        "rawSignals": [{"sample": index} for index in range(1200)],
        "userLocationProfile": {"places": [{"other": "not supported"}]},
        'odd.key["x"]': {"extra": False},
        "empty": [],
        "last": None,
    }
    await setup_import(hass, payload)
    result = await call_import(hass)
    records = read_log(result["log_file"])
    items = [record for record in records if record["event"] == "item"]
    by_path = {record["json_path"]: record for record in items}
    assert by_path["$.before[0]"]["data"] == payload["before"][0]
    for index, value in enumerate(payload["rawSignals"]):
        assert by_path[f"$.rawSignals[{index}]"]["data"] == value
    for key in ("userLocationProfile", 'odd.key["x"]', "empty", "last"):
        json_path = services._google_json_path("$", key)
        assert by_path[json_path]["data"] == payload[key]
    inventory = records[-1]["field_inventory"]
    assert inventory["$.before[*].nested[*].name"] == {"str": 1}
    assert inventory["$.rawSignals[*].sample"] == {"int": 1200}
    assert inventory["$.userLocationProfile.places[*].other"] == {"str": 1}
    assert inventory["$.semanticSegments[*].activity.topCandidate.probability"] == {"float": 2}
    assert result["unsupported_items"] == 1204
    assert result["points_inserted"] == 5


async def test_unused_segment_point_and_coordinate_fields_have_exact_values(hass):
    segment = {
        "startTime": timestamp(10),
        "endTime": timestamp(12),
        "newSegmentKind": {"future": [1, {"other": "value"}]},
        "timelinePath": [
            {
                "point": {"latLng": "50°, 8°", "accuracy": {"meters": 42}},
                "time": timestamp(10),
                "durationMinutesOffsetFromStartTime": 7,
                "extra": ["all", "values"],
            }
        ],
    }
    await setup_import(hass, {"semanticSegments": [segment]})
    result = await call_import(hass)
    records = read_log(result["log_file"])
    by_path = {record["json_path"]: record for record in records if record["event"] == "item"}
    prefix = "$.semanticSegments[0]"
    assert by_path[f"{prefix}.newSegmentKind"]["data"] == segment["newSegmentKind"]
    assert by_path[f"{prefix}.timelinePath[0].extra"]["data"] == ["all", "values"]
    assert by_path[f"{prefix}.timelinePath[0].point.accuracy"]["data"] == {"meters": 42}
    assert by_path[f"{prefix}.timelinePath[0].durationMinutesOffsetFromStartTime"]["data"] == 7
    assert by_path[prefix]["code"] == "segment_metadata_not_archived"
    assert result["points_inserted"] == 1


@pytest.mark.parametrize("damage", ["coordinates", "time", "missing", "not_object"])
async def test_bad_point_is_logged_without_losing_valid_neighbors(hass, damage):
    bad = {"point": "50°, 8°", "time": timestamp(11)}
    if damage == "coordinates":
        bad["point"] = "91°, 8°"
    elif damage == "time":
        bad["time"] = "2020-01-02T11:00:00"
    elif damage == "missing":
        del bad["point"]
    else:
        bad = ["unexpected", "array"]
    segment = {
        "timelinePath": [
            {"point": "50°, 8°", "time": timestamp(10)},
            bad,
            {"point": "51°, 9°", "time": timestamp(12)},
        ]
    }
    await setup_import(hass, {"semanticSegments": [segment]})
    result = await call_import(hass)
    records = read_log(result["log_file"])
    invalid = log_items(records, "invalid_point")
    assert len(invalid) == result["invalid_items"] == 1
    assert invalid[0]["data"] == bad
    assert invalid[0]["json_path"] == "$.semanticSegments[0].timelinePath[1]"
    assert invalid[0]["reason"]
    assert result["points_inserted"] == 2
    assert records[-1]["parse_complete"] is True


async def test_invalid_semantic_items_do_not_leak_points_or_hide_valid_siblings(hass):
    segment = visit()
    segment["activity"] = activity(timestamp(10), timestamp(11))["activity"]
    segment["activity"]["end"] = {"latLng": "200°, 9°"}
    invalid_visit = visit(12, 13, hierarchyLevel="invalid")
    payload = {
        "semanticSegments": [
            segment,
            invalid_visit,
            42,
            {"timelinePath": "not an array"},
            {"timelinePath": []},
            {"unknownSegment": {"all": "preserved"}},
            activity(timestamp(14), timestamp(15)),
        ],
        "last": {"also": "seen"},
    }
    _, path = await setup_import(hass, payload)
    result = await call_import(hass)
    records = read_log(result["log_file"])
    assert log_items(records, "invalid_activity")[0]["data"] == segment["activity"]
    assert log_items(records, "invalid_visit")[0]["data"] == invalid_visit["visit"]
    assert log_items(records, "invalid_segment")[0]["data"] == 42
    assert log_items(records, "invalid_timeline_path")[0]["data"] == "not an array"
    assert log_items(records, "empty_timeline_path")
    assert log_items(records, "unsupported_segment")
    assert result["invalid_items"] == 4
    assert result["points_inserted"] == 4
    # The valid start of the rejected activity must not displace the visit location.
    assert archive_snapshot(path)["points"][0][4:6] == (51, 9)
    assert result["visits_read"] == result["activities_read"] == 1


async def test_missing_history_target_still_scans_every_section_and_logs_rollback(hass):
    _, path = await setup_import(hass, companions=False)
    before = archive_snapshot(path)
    with pytest.raises(HomeAssistantError, match="Diagnostic log:"):
        await call_import(hass)
    records = read_log(only_log(path))
    assert len(log_items(records, "missing_history_target")) == 3
    assert (
        log_items(records, "unsupported_top_level_item")[0]["data"]
        == export()["rawSignals"][0]
    )
    assert len(log_items(records, "import_aborted")) == 3
    assert records[-1]["parse_complete"] is True
    assert records[-1]["committed"] is False
    assert archive_snapshot(path) == before


async def test_duplicate_points_intervals_and_zero_duration_include_original_values(hass):
    segment = export()["semanticSegments"][-1]
    duplicate = deepcopy(segment["timelinePath"][0])
    segment["timelinePath"].append(duplicate)
    await setup_import(hass, {"semanticSegments": [visit(), visit(), segment, visit(15, 15)]})
    result = await call_import(hass)
    records = read_log(result["log_file"])
    assert log_items(records, "duplicate_interval")[0]["data"] == visit()["visit"]
    assert any(item["data"] == duplicate for item in log_items(records, "duplicate_point"))
    collisions = log_items(records, "point_timestamp_collision")
    assert any(item["json_path"].endswith(".placeLocation") for item in collisions)
    assert any(item["data"] == segment["timelinePath"][2] for item in collisions)
    assert all(item["kept_json_path"] for item in collisions)
    assert log_items(records, "zero_duration_interval")[0]["data"] == visit(15, 15)["visit"]
    assert result["points_inserted"] == 4


async def test_shadowed_duplicate_json_members_are_preserved_in_log(hass):
    _, path = await setup_import(hass)
    text = json.dumps({"semanticSegments": [visit()]})
    text = text.replace(
        '"hierarchyLevel": 0', '"hierarchyLevel": {"old": 7}, "hierarchyLevel": 0'
    )
    path.with_name("Timeline.json").write_text(text)
    result = await call_import(hass)
    records = read_log(result["log_file"])
    duplicate = log_items(records, "duplicate_json_key")
    assert len(duplicate) == 1
    assert duplicate[0]["json_path"] == "$.semanticSegments[0].visit.hierarchyLevel"
    assert duplicate[0]["data"] == {"old": 7}
    assert records[-1]["field_inventory"][
        "$.semanticSegments[*].visit.hierarchyLevel.old"
    ] == {"int": 1}
    assert result["visit_states_inserted"] == 2


async def test_syntax_failure_preserves_complete_unparsed_tail_and_accepted_sources(hass):
    _, path = await setup_import(hass)
    unread = '{"broken": ' + '"unfinished ' * 12000
    text = '{"semanticSegments":[' + json.dumps(visit()) + "," + unread
    path.with_name("Timeline.json").write_text(text)
    before = archive_snapshot(path)
    with pytest.raises(HomeAssistantError, match="Diagnostic log:"):
        await call_import(hass)
    records = read_log(only_log(path))
    chunks = log_items(records, "unparsed_json_text")
    assert "".join(item["data"] for item in chunks) == unread
    offset = text.index(unread)
    for chunk in chunks:
        assert chunk["character_offset"] == offset
        offset += len(chunk["data"])
    assert log_items(records, "json_read_failed")[0]["json_path"] == "$.semanticSegments[1]"
    assert len(log_items(records, "import_aborted")) == 3
    assert records[-1]["parse_complete"] is records[-1]["committed"] is False
    assert records[-1]["result"]["points_inserted"] == 0
    assert archive_snapshot(path) == before


@pytest.mark.parametrize(
    "overrides", [{"entity_id": "device_tracker.missing"}, {"file_name": "missing.json"}]
)
async def test_preflight_errors_also_create_closed_diagnostic_logs(hass, overrides):
    _, path = await setup_import(hass)
    with pytest.raises(HomeAssistantError, match="Diagnostic log:") as err:
        await call_import(hass, **overrides)
    logfile = only_log(path)
    assert str(logfile) in str(err.value)
    records = read_log(logfile)
    assert records[-1]["status"] == "failed"
    assert records[-1]["error"]
    assert records[-1]["committed"] is records[-1]["parse_complete"] is False


async def test_entirely_unknown_export_is_still_fully_logged(hass):
    payload = {"otherExport": [{"unknown": 1}, {"last": 2}], "metadata": {"version": 99}}
    _, path = await setup_import(hass, payload)
    before = archive_snapshot(path)
    with pytest.raises(HomeAssistantError, match="semanticSegments"):
        await call_import(hass)
    records = read_log(only_log(path))
    assert [r["data"] for r in log_items(records, "unsupported_top_level_item")] == payload[
        "otherExport"
    ]
    assert log_items(records, "unsupported_top_level_field")[0]["data"] == payload["metadata"]
    assert records[-1]["parse_complete"] is True
    assert records[-1]["committed"] is False
    assert archive_snapshot(path) == before


async def test_cancellation_while_creating_log_closes_it_without_starting_import(
    hass, monkeypatch
):
    _, path = await setup_import(hass)
    before = archive_snapshot(path)
    entered, resume = threading.Event(), threading.Event()
    create = services._GoogleImportLog

    def pause(*args):
        audit = create(*args)
        entered.set()
        assert resume.wait(10)
        return audit

    monkeypatch.setattr(services, "_GoogleImportLog", pause)
    task = asyncio.create_task(call_import(hass))
    assert await hass.async_add_executor_job(entered.wait, 10)
    task.cancel()
    await asyncio.sleep(0)
    resume.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    records = read_log(only_log(path))
    assert records[-1]["status"] == "cancelled"
    assert records[-1]["committed"] is False
    assert archive_snapshot(path) == before


def raw_position(time, coordinates="50.25°, 8.25°", **extra):
    return {
        "position": {
            "LatLng": coordinates,
            "timestamp": time,
            "source": "GPS",
            "accuracyMeters": 6,
            "altitudeMeters": 500.5,
            "speedMetersPerSecond": 2.5,
            **extra,
        }
    }


def raw_activity(time, kind="WALKING", confidence=0.9, candidates=None):
    return {
        "activityRecord": {
            "timestamp": time,
            "probableActivities": candidates
            if candidates is not None
            else [
                {"type": kind, "confidence": confidence},
            ],
        }
    }


@pytest.mark.parametrize("raw_first", [False, True])
async def test_raw_positions_fill_gaps_with_measurements_and_consistent_priority(
    hass, raw_first
):
    raw = [
        raw_position(timestamp(10)),  # Beats visit, but loses to timelinePath.
        raw_position(timestamp(11)),  # Beats activity endpoint.
        raw_position(timestamp(13)),
        raw_position(timestamp(13), "60°, 6°"),  # First equal-priority observation wins.
    ]
    payload = {
        "semanticSegments": [
            visit(),
            activity(timestamp(11), timestamp(12)),
            {"timelinePath": [{"time": timestamp(10), "point": "50°, 8°"}]},
        ],
        "rawSignals": raw,
    }
    if raw_first:
        payload = {"rawSignals": raw, "semanticSegments": payload["semanticSegments"]}
    _, path = await setup_import(hass, payload)
    before = archive_snapshot(path)
    result = await call_import(hass)
    assert result["raw_positions_read"] == 4
    assert result["points_inserted"] == 4
    with sqlite3.connect(path) as conn:
        rows = conn.execute(
            "SELECT lat,lon,accuracy,speed,altitude,attributes FROM points ORDER BY ts"
        ).fetchall()
    assert rows[0][:5] == (50, 8, None, None, None)
    assert rows[1][:5] == rows[3][:5] == (50.25, 8.25, 6, 2.5, 500.5)
    assert json.loads(rows[3][5])["google_timeline"]["position"] == raw[2]["position"]
    assert archive_snapshot(path)["schema"] == before["schema"]
    second = await call_import(hass)
    assert second["points_inserted"] == 0
    assert len(log_items(read_log(second["log_file"]), "existing_point")) == 4


@pytest.mark.parametrize("raw_first", [False, True])
async def test_raw_activity_observations_do_not_interrupt_semantic_intervals(hass, raw_first):
    raw = [raw_activity(timestamp(hour), "RUNNING") for hour in range(8, 16)]
    payload = {
        "semanticSegments": [
            activity(timestamp(10), timestamp(12), "IN_VEHICLE"),
            activity(timestamp(11), timestamp(13), "CYCLING"),
            activity(timestamp(15), timestamp(16), "STILL"),
        ],
        "rawSignals": raw,
    }
    if raw_first:
        payload = {"rawSignals": raw, "semanticSegments": payload["semanticSegments"]}
    await setup_import(hass, payload)
    result = await call_import(hass)
    records = read_log(result["log_file"])
    assert result["raw_activity_records_read"] == 8
    assert len(log_items(records, "raw_activity_covered_by_interval")) == 4
    assert len(log_items(records, "raw_activity_at_semantic_boundary")) == 1
    history = await hass.data[DOMAIN]["store"].async_query_states([ACTIVITY], 0, 2_000_000_000)
    assert [row["s"] for row in history[ACTIVITY]] == [
        "running",
        "running",
        "automotive",
        "cycling",
        "cycling",
        "unknown",
        "running",
        "stationary",
        "unknown",
    ]
    assert (
        history[ACTIVITY][0]["a"]["google_timeline"]["activityRecord"]
        == raw[0]["activityRecord"]
    )
    assert history[ACTIVITY][0]["a"]["confidence"] == 0.9
    second = await call_import(hass)
    assert second["activity_states_inserted"] == 0


async def test_raw_activity_confidence_specificity_duplicates_and_invalid_siblings(hass):
    raw = [
        raw_activity(
            timestamp(8),
            candidates=[
                {"type": "ON_FOOT", "confidence": 0.8},
                {"type": "RUNNING", "confidence": 0.8},
                {"type": "WALKING", "confidence": 0.7},
                {"type": "STILL", "confidence": 5},
            ],
        ),
        raw_activity(timestamp(9), "WALKING", 0.5),
        raw_activity(timestamp(9), "STILL", 0.9),
        raw_activity(timestamp(9), "RUNNING", 0.9),
        raw_activity(timestamp(10), "UNKNOWN", 1),
        raw_activity(timestamp(11), candidates=[]),
        {"position": {"timestamp": timestamp(12), "LatLng": "91°, 8°"}},
        raw_position(timestamp(13), accuracyMeters="invalid", altitudeMeters=-2),
    ]
    await setup_import(hass, {"semanticSegments": [], "rawSignals": raw})
    result = await call_import(hass)
    assert result["activity_states_inserted"] == 3
    assert result["points_inserted"] == 1
    assert result["raw_activity_records_read"] == 5
    assert result["invalid_items"] == 4
    records = read_log(result["log_file"])
    assert len(log_items(records, "raw_activity_timestamp_collision")) == 2
    history = await hass.data[DOMAIN]["store"].async_query_states(
        [ACTIVITY, ENTITY], 0, 2_000_000_000
    )
    assert [row["s"] for row in history[ACTIVITY]] == ["running", "stationary", "unknown"]
    assert (
        history[ACTIVITY][0]["a"]["google_timeline"]["activityRecord"]
        == raw[0]["activityRecord"]
    )
    assert history[ENTITY][0]["a"]["altitude"] == -2
    assert "gps_accuracy" not in history[ENTITY][0]["a"]


async def test_aggregate_data_have_specific_reasons_and_no_fabricated_archive_rows(hass):
    memory = {
        "startTime": timestamp(8),
        "endTime": timestamp(12),
        "timelineMemory": {
            "trip": {
                "distanceFromOriginKms": 100,
                "destinations": [{"identifier": {"placeId": "synthetic"}}],
            }
        },
    }
    wifi = {
        "wifiScan": {
            "deliveryTime": timestamp(8),
            "devicesRecords": [{"mac": 12345, "rawRssi": -50}],
        }
    }
    profile = {"frequentPlaces": [{"placeId": "synthetic", "placeLocation": "50°, 8°"}]}
    _, path = await setup_import(
        hass,
        {
            "semanticSegments": [memory],
            "rawSignals": [wifi],
            "userLocationProfile": profile,
        },
        companions=False,
    )
    result = await call_import(hass)
    records = read_log(result["log_file"])
    assert result["invalid_items"] == result["unsupported_items"] == 0
    assert result["points_inserted"] == result["activity_states_inserted"] == 0
    assert (
        log_items(records, "trip_summary_without_track")[0]["data"] == memory["timelineMemory"]
    )
    assert log_items(records, "wifi_scan_without_position")[0]["data"] == wifi["wifiScan"]
    assert log_items(records, "profile_without_observation_time")[0]["data"] == profile
    assert archive_snapshot(path)["version"] == 2


async def test_raw_records_are_included_in_rollback_log_on_later_syntax_error(hass):
    _, path = await setup_import(hass)
    payload = {
        "rawSignals": [raw_position(timestamp(10)), raw_activity(timestamp(11))],
        "semanticSegments": [],
    }
    path.with_name("Timeline.json").write_text(json.dumps(payload) + " broken")
    before = archive_snapshot(path)
    with pytest.raises(HomeAssistantError):
        await call_import(hass)
    records = read_log(only_log(path))
    assert len(log_items(records, "import_aborted")) == 2
    assert records[-1]["committed"] is False
    assert archive_snapshot(path) == before


async def test_raw_activity_requires_existing_target_and_still_scans_to_end(hass):
    _, path = await setup_import(
        hass,
        {
            "semanticSegments": [],
            "rawSignals": [raw_activity(timestamp(10)), raw_position(timestamp(11))],
            "after": {"must": "be logged"},
        },
        companions=False,
    )
    with pytest.raises(HomeAssistantError, match="activity_entity"):
        await call_import(hass)
    records = read_log(only_log(path))
    assert log_items(records, "missing_history_target")
    assert log_items(records, "unsupported_top_level_field")[0]["data"] == {
        "must": "be logged"
    }
    assert records[-1]["parse_complete"] is True
    assert records[-1]["committed"] is False


async def test_parking_goes_only_to_car_at_its_own_time_and_can_be_imported_later(hass):
    payload = export()
    parking = [
        parked_activity(timestamp(11)),  # Same timestamp as a different person coordinate.
        parked_activity("2020-01-03T00:30:00+02:00", "55°, 13°"),
    ]
    payload["semanticSegments"].extend(parking)
    await hass.config.async_set_time_zone("Europe/Berlin")
    _, path = await setup_import(hass, payload)
    await setup_car(hass)
    entities = set(er.async_get(hass).entities)
    live_states = set(hass.states.async_entity_ids())
    before = archive_snapshot(path)
    # An existing car configuration alone must not implicitly select it.
    first = await call_import(hass)
    assert first["parking_records_read"] == first["parking_points_skipped"] == 2
    assert first["parking_points_inserted"] == first["parking_days"] == 0
    records = read_log(first["log_file"])
    skipped = log_items(records, "parking_without_car_target")
    assert [item["data"] for item in skipped] == [s["activity"]["parking"] for s in parking]
    store = hass.data[DOMAIN]["store"]
    person = await store.async_query_states([ENTITY, PLACES, ACTIVITY], 0, 2_000_000_000)
    assert await store.async_query_states([CAR], 0, 2_000_000_000) == {}

    second = await call_import(hass, car_entity_id=CAR)
    assert second["car_entity_id"] == CAR
    assert second["points_inserted"] == second["days"] == 0
    assert second["visit_states_inserted"] == second["activity_states_inserted"] == 0
    assert second["parking_points_inserted"] == 2
    assert second["parking_points_skipped"] == 0
    # The second UTC/local timestamp is on January 2 in HA's Berlin timezone.
    assert second["parking_days"] == 1
    assert (
        await store.async_query_states([ENTITY, PLACES, ACTIVITY], 0, 2_000_000_000) == person
    )
    car = (await store.async_query_states([CAR], 0, 2_000_000_000))[CAR]
    assert [p["lu"] for p in car] == [
        datetime.fromisoformat(s["activity"]["parking"]["startTime"]).timestamp()
        for s in parking
    ]
    assert [p["a"]["latitude"] for p in car] == [54.123457, 55.0]
    assert [p["a"]["google_timeline"]["parking"] for p in car] == [
        s["activity"]["parking"] for s in parking
    ]
    assert all(p["a"]["google_timeline_type"] == "parking" for p in car)
    with sqlite3.connect(path) as conn:
        assert (
            conn.execute(
                "SELECT COUNT(*) FROM entity_states AS s JOIN trackers AS t"
                " ON s.tracker_id=t.id WHERE t.entity_id=?",
                (CAR,),
            ).fetchone()[0]
            == 0
        )
    after = archive_snapshot(path)
    assert after["schema"] == before["schema"]
    assert after["version"] == 2
    assert set(er.async_get(hass).entities) == entities
    assert set(hass.states.async_entity_ids()) == live_states
    again = await call_import(hass, car_entity_id=CAR)
    assert again["parking_points_inserted"] == again["parking_days"] == 0
    assert again["parking_points_skipped"] == 2
    assert len(log_items(read_log(again["log_file"]), "existing_parking_point")) == 2
    assert archive_snapshot(path) == after


async def test_parking_duplicates_invalid_items_and_invalid_parent_are_independent(hass):
    segments = [
        parked_activity(timestamp(12)),
        parked_activity(timestamp(12)),
        parked_activity(timestamp(12), "56°, 14°"),
        parked_activity(timestamp(13)),
        parked_activity(timestamp(14)),
    ]
    # Parking has its own timestamp and coordinates even if the activity is invalid.
    segments[4]["endTime"] = "invalid parent time"
    invalid = [
        None,
        {},
        {"startTime": timestamp(15), "location": "91°, 2°"},
        {"startTime": "2020-01-02T15:00:00", "location": "55°, 13°"},
    ]
    for value in invalid:
        segment = parked_activity(timestamp(15))
        segment["activity"]["parking"] = value
        segments.append(segment)
    _, path = await setup_import(hass, {"semanticSegments": segments})
    await setup_car(hass)
    with sqlite3.connect(path) as conn:
        car_id = conn.execute("SELECT id FROM trackers WHERE entity_id=?", (CAR,)).fetchone()[
            0
        ]
        conn.execute(
            "INSERT INTO points(tracker_id, ts, state, lat, lon, attributes)"
            " VALUES (?, ?, 'home', 60, 15, '{}')",
            (car_id, datetime.fromisoformat(timestamp(13)).timestamp()),
        )
        existing = conn.execute(
            "SELECT * FROM points WHERE tracker_id=?", (car_id,)
        ).fetchone()
    result = await call_import(hass, car_entity_id=CAR)
    assert result["parking_records_read"] == 9
    assert result["parking_points_inserted"] == 2
    assert result["parking_points_skipped"] == 7
    assert result["invalid_items"] == 5  # Four parking items and one parent activity.
    records = read_log(result["log_file"])
    assert [r["data"] for r in log_items(records, "invalid_parking")] == invalid
    assert len(log_items(records, "duplicate_parking_point")) == 1
    collision = log_items(records, "parking_timestamp_collision")[0]
    assert collision["data"] == segments[2]["activity"]["parking"]
    assert collision["entity_id"] == CAR
    assert collision["kept_json_path"] == "$.semanticSegments[0].activity.parking"
    assert len(log_items(records, "existing_parking_point")) == 1
    with sqlite3.connect(path) as conn:
        rows = conn.execute(
            "SELECT * FROM points WHERE tracker_id=? ORDER BY ts", (car_id,)
        ).fetchall()
    assert len(rows) == 3
    assert rows[1] == existing
    assert rows[0][4:6] == (54.123457, 12.2)


@pytest.mark.parametrize(
    ("overrides", "error"),
    [
        ({"car_entity_id": ENTITY}, "must differ"),
        ({"car_entity_id": PLACES}, "device_tracker entity"),
        ({"car_entity_id": CAR}, "not tracked"),
        ({"car_entity_id": CAR, CONF_PLACES_ENTITY: CAR}, "must differ"),
    ],
)
async def test_car_selection_rejects_ambiguous_or_unconfigured_targets(hass, overrides, error):
    _, path = await setup_import(hass)
    before = archive_snapshot(path)
    with pytest.raises(HomeAssistantError, match=error):
        await call_import(hass, **overrides)
    assert archive_snapshot(path) == before
    assert read_log(only_log(path))[-1]["committed"] is False


async def test_failure_after_car_insert_rolls_back_person_and_car_with_full_log(
    hass, monkeypatch
):
    payload = {"semanticSegments": [parked_activity(timestamp(12))]}
    _, path = await setup_import(hass, payload)
    await setup_car(hass)
    before = archive_snapshot(path)
    connect = sqlite3.connect

    class FailAfterCar(sqlite3.Connection):
        def execute(self, sql, *args, **kwargs):
            result = super().execute(sql, *args, **kwargs)
            if sql.startswith("INSERT OR IGNORE INTO points") and "google_parking" in sql:
                raise sqlite3.OperationalError("simulated error after writing both trackers")
            return result

    monkeypatch.setattr(
        services.sqlite3, "connect", lambda *a, **kw: connect(*a, **kw, factory=FailAfterCar)
    )
    with pytest.raises(HomeAssistantError, match="writing both trackers"):
        await call_import(hass, car_entity_id=CAR)
    assert archive_snapshot(path) == before
    records = read_log(only_log(path))
    assert records[-1]["committed"] is False
    assert records[-1]["result"]["points_inserted"] == 0
    assert records[-1]["result"]["parking_points_inserted"] == 0
    aborted = [
        item for item in log_items(records, "import_aborted") if item.get("entity_id") == CAR
    ]
    assert len(aborted) == 1
    assert aborted[0]["data"] == payload["semanticSegments"][0]["activity"]["parking"]


async def test_car_archive_binding_is_rechecked_before_commit(hass, monkeypatch):
    _, path = await setup_import(hass, {"semanticSegments": [parked_activity(timestamp(12))]})
    await setup_car(hass)
    before = archive_snapshot(path)
    stage = services._stage_google_segment

    def unbind_during_staging(*args):
        counts = stage(*args)
        with sqlite3.connect(path) as other:
            other.execute("UPDATE trackers SET entry_id=NULL WHERE entity_id=?", (CAR,))
        return counts

    monkeypatch.setattr(services, "_stage_google_segment", unbind_during_staging)
    with pytest.raises(HomeAssistantError, match="no longer separately bound"):
        await call_import(hass, car_entity_id=CAR)
    assert archive_snapshot(path) == before
    assert read_log(only_log(path))[-1]["committed"] is False
