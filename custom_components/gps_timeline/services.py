from __future__ import annotations

import asyncio
from collections.abc import Iterator
import contextlib
from datetime import datetime, timedelta
import functools
from itertools import groupby
import json
import logging
import math
from pathlib import Path
import sqlite3
import threading
from typing import TextIO
from zoneinfo import ZoneInfo

from homeassistant.components.recorder import get_instance
from homeassistant.components.recorder.history import get_significant_states
from homeassistant.core import HomeAssistant, ServiceCall, SupportsResponse, callback
from homeassistant.exceptions import HomeAssistantError
from homeassistant.helpers import config_validation as cv
from homeassistant.util import dt as dt_util
import voluptuous as vol

from .const import (
    CONF_ACCURACY_THRESHOLD,
    CONF_ACTIVITY_ENTITY,
    CONF_ATTACH_TRACKER_ID,
    CONF_ENTITY_ID,
    CONF_PLACES_ENTITY,
    COORD_DECIMALS,
    DB_DIR_NAME,
    DB_FILE_NAME,
    DEFAULT_ACCURACY_THRESHOLD,
    DOMAIN,
    SERVICE_BACKFILL,
    SERVICE_PURGE,
)
from .helpers import (
    async_purge_entry_data,
    entry_setting,
    live_entity_claims,
    live_entry_ids,
    tracked_entity_ids,
)
from .store import SCHEMA_VERSION, Store, normalize_entity_state, normalize_point

BACKFILL_SCHEMA = vol.Schema(
    {
        vol.Required(CONF_ENTITY_ID): cv.entity_id,
        vol.Optional("start_time"): cv.datetime,
        vol.Optional("end_time"): cv.datetime,
    }
)

PURGE_SCHEMA = vol.Schema(
    {
        vol.Required(CONF_ENTITY_ID): cv.entity_id,
    }
)

GOOGLE_IMPORT_SERVICE = "import_google_timeline"
GOOGLE_IMPORT_SCHEMA = vol.Schema(
    {
        vol.Required(CONF_ENTITY_ID): cv.entity_id,
        vol.Required("file_name"): cv.string,
        vol.Optional(CONF_PLACES_ENTITY): cv.entity_id,
        vol.Optional(CONF_ACTIVITY_ENTITY): cv.entity_id,
    }
)

DEFAULT_BACKFILL_DAYS = 10

_LOGGER = logging.getLogger(__name__)


def _find_entry(hass: HomeAssistant, entity_id: str):
    for entry in hass.config_entries.async_entries(DOMAIN):
        if entry.data.get(CONF_ENTITY_ID, "").lower() == entity_id:
            return entry
    return None


def _recorder_instance(hass: HomeAssistant):
    try:
        return get_instance(hass)
    except KeyError as err:
        raise HomeAssistantError(
            "The recorder integration is not available; backfill requires it"
        ) from err


@callback
def async_register_services(hass: HomeAssistant) -> None:
    hass.services.async_register(
        DOMAIN, SERVICE_BACKFILL, _async_handle_backfill, schema=BACKFILL_SCHEMA
    )
    hass.services.async_register(
        DOMAIN, SERVICE_PURGE, _async_handle_purge, schema=PURGE_SCHEMA
    )
    hass.services.async_register(
        DOMAIN,
        GOOGLE_IMPORT_SERVICE,
        _async_handle_google_import,
        schema=GOOGLE_IMPORT_SCHEMA,
        supports_response=SupportsResponse.OPTIONAL,
    )


async def _async_handle_backfill(call: ServiceCall) -> None:
    hass = call.hass
    entity_id = call.data[CONF_ENTITY_ID].lower()
    entry = _find_entry(hass, entity_id)
    if entry is None:
        raise HomeAssistantError(f"{entity_id} is not tracked by GPS Timeline")

    store: Store | None = hass.data[DOMAIN].get("store")
    if store is None:
        raise HomeAssistantError("GPS Timeline store is not available")

    end_time = dt_util.as_utc(call.data.get("end_time") or dt_util.utcnow())
    start_time = dt_util.as_utc(
        call.data.get("start_time") or (end_time - timedelta(days=DEFAULT_BACKFILL_DAYS))
    )
    if start_time > end_time:
        raise HomeAssistantError("start_time must be before end_time")

    tracker_id = await store.async_bind_tracker(entry.entry_id, entity_id)
    query_entity_ids = tracked_entity_ids(entry)
    threshold = entry_setting(entry, CONF_ACCURACY_THRESHOLD, DEFAULT_ACCURACY_THRESHOLD)

    result = await _recorder_instance(hass).async_add_executor_job(
        functools.partial(
            get_significant_states,
            hass,
            start_time=start_time,
            end_time=end_time,
            entity_ids=query_entity_ids,
            include_start_time_state=False,
            significant_changes_only=False,
            no_attributes=False,
        )
    )

    points: list[tuple[int, tuple]] = []
    states: list[tuple[int, str, tuple]] = []
    for state_entity_id, history_states in result.items():
        if state_entity_id == entity_id:
            for state in history_states:
                if row := normalize_point(state, threshold):
                    points.append((tracker_id, row))
        else:
            for state in history_states:
                if row := normalize_entity_state(state):
                    states.append((tracker_id, state_entity_id, row))

    inserted, total = await store.async_write_backfill(points, states)
    _LOGGER.info(
        "Backfill for %s: %s of %s rows imported (%s points, %s companion states)",
        entity_id,
        inserted,
        total,
        len(points),
        len(states),
    )


async def _async_handle_purge(call: ServiceCall) -> None:
    """Delete archived tracker data for an entity (destructive)."""
    hass = call.hass
    entity_id = call.data[CONF_ENTITY_ID].lower()

    data = hass.data.get(DOMAIN)
    store: Store | None = data.get("store") if isinstance(data, dict) else None
    if store is None:
        raise HomeAssistantError(
            "GPS Timeline store is not available; it only runs while at least"
            " one tracker is loaded"
        )

    tracker = await store.async_get_tracker_by_entity(entity_id)
    if tracker is None:
        raise HomeAssistantError(f"No archived GPS Timeline data for {entity_id}")

    owner_entry_id = tracker.get("entry_id")
    if owner_entry_id in set(live_entry_ids(hass)):
        # Purging a live tracker wipes its history; drop the stale adoption
        # provenance (its data no longer exists) and reload so archiving
        # continues with a fresh, empty timeline.
        entry = hass.config_entries.async_get_entry(owner_entry_id)
        if entry is not None and CONF_ATTACH_TRACKER_ID in entry.data:
            hass.config_entries.async_update_entry(
                entry,
                data={
                    key: value
                    for key, value in entry.data.items()
                    if key != CONF_ATTACH_TRACKER_ID
                },
            )
        await async_purge_entry_data(
            hass, entry_id=owner_entry_id, exclude_entry_id=owner_entry_id
        )
        hass.config_entries.async_schedule_reload(owner_entry_id)
        return

    await async_purge_entry_data(hass, tracker_id=int(tracker["tracker_id"]))


async def _async_handle_google_import(call: ServiceCall) -> dict:
    """Import an on-device Google export without changing entities or the schema."""
    hass = call.hass
    entity_id = call.data[CONF_ENTITY_ID].lower()
    entry = _find_entry(hass, entity_id)
    if entry is None:
        raise HomeAssistantError(f"{entity_id} is not tracked by GPS Timeline")
    store = hass.data.get(DOMAIN, {}).get("store")
    if store is None:
        raise HomeAssistantError("GPS Timeline store is not available")

    targets = {
        kind: (call.data.get(key) or entry_setting(entry, key) or "").lower()
        for kind, key in (("visit", CONF_PLACES_ENTITY), ("activity", CONF_ACTIVITY_ENTITY))
    }
    assigned = [target for target in targets.values() if target]
    if entity_id in assigned or len(assigned) != len(set(assigned)):
        raise HomeAssistantError("Tracker, Places and activity entity IDs must differ")
    claims = live_entity_claims(hass)
    for target in assigned:
        if claims.get(target, entry.entry_id) != entry.entry_id:
            raise HomeAssistantError(f"{target} is already tracked by another entry")

    await store.async_flush()
    cancelled = threading.Event()
    job = hass.async_add_executor_job(
        _import_google_file,
        Path(hass.config.path(DB_DIR_NAME, DB_FILE_NAME)),
        call.data["file_name"],
        entry.entry_id,
        entity_id,
        targets,
        hass.config.time_zone,
        cancelled,
    )
    try:
        # wait() leaves the worker future intact when the service is cancelled.
        await asyncio.wait([job])
        result = job.result()
    except asyncio.CancelledError:
        cancelled.set()
        # Do not leave a writer running after the service has been cancelled.
        with contextlib.suppress(Exception):
            await job
        raise
    except (OSError, ValueError, sqlite3.Error) as err:
        raise HomeAssistantError(f"Google Timeline import failed: {err}") from err
    _LOGGER.info("Google Timeline import for %s: %s", entity_id, result)
    return result


class _GoogleJSONReader:
    """Decode one array item at a time, including exports larger than available RAM."""

    def __init__(self, stream: TextIO, cancelled: threading.Event | None = None) -> None:
        self.stream = stream
        self.cancelled = cancelled
        self.buffer = ""
        self.pos = 0
        self.eof = False
        self.decoder = json.JSONDecoder(parse_constant=self._invalid_constant)

    @staticmethod
    def _invalid_constant(value: str):
        raise ValueError(f"Invalid JSON number: {value}")

    def _read(self) -> None:
        if self.cancelled is not None and self.cancelled.is_set():
            raise ValueError("Import cancelled")
        chunk = self.stream.read(64 * 1024)
        self.buffer = self.buffer[self.pos :] + chunk
        self.pos = 0
        self.eof = not chunk

    def peek(self) -> str:
        while True:
            while self.pos < len(self.buffer):
                char = self.buffer[self.pos]
                if char not in " \t\r\n":
                    return char
                self.pos += 1
            if self.eof:
                return ""
            self._read()

    def take(self, char: str) -> None:
        if self.peek() != char:
            raise ValueError(f"Invalid or incomplete JSON: expected {char!r}")
        self.pos += 1

    def value(self):
        self.peek()
        while True:
            try:
                value, end = self.decoder.raw_decode(self.buffer, self.pos)
            except json.JSONDecodeError:
                if self.eof:
                    raise ValueError("Invalid or incomplete JSON value") from None
            else:
                # A number at a chunk boundary may continue in the next chunk.
                if (end < len(self.buffer) and self.buffer[end] in " \t\r\n,]}:") or (
                    end == len(self.buffer) and self.eof
                ):
                    self.pos = end
                    return value
                if self.eof:
                    raise ValueError("Invalid JSON value delimiter")
            self._read()

    def array(self) -> Iterator:
        self.take("[")
        if self.peek() != "]":
            while True:
                yield self.value()
                if self.peek() == "]":
                    break
                self.take(",")
        self.take("]")

    def segments(self) -> Iterator[dict]:
        self.take("{")
        found = False
        keys: set[str] = set()
        if self.peek() != "}":
            while True:
                key = self.value()
                if not isinstance(key, str) or key in keys:
                    raise ValueError("Invalid or duplicate top-level JSON key")
                keys.add(key)
                self.take(":")
                if key == "semanticSegments":
                    found = True
                    yield from self.array()
                elif self.peek() == "[":
                    # rawSignals can be very large too; validate and discard each item.
                    for _ in self.array():
                        pass
                else:
                    self.value()
                if self.peek() == "}":
                    break
                self.take(",")
        self.take("}")
        if not found or self.peek():
            raise ValueError("Expected one JSON object containing semanticSegments")


def _google_time(value) -> datetime:
    if not isinstance(value, str):
        raise ValueError("Missing or invalid timestamp")
    result = datetime.fromisoformat(value)
    if result.tzinfo is None:
        raise ValueError("Timestamps must include a UTC offset")
    return result


def _google_coordinates(value) -> tuple[float, float]:
    if isinstance(value, dict):
        value = value.get("latLng")
    if not isinstance(value, str):
        raise ValueError("Missing or invalid coordinates")
    parts = value.removeprefix("geo:").replace("°", "").split(",")
    if len(parts) != 2:
        raise ValueError("Expected latitude, longitude")
    lat, lon = map(float, parts)
    if not (
        math.isfinite(lat) and math.isfinite(lon) and -90 <= lat <= 90 and -180 <= lon <= 180
    ):
        raise ValueError("Coordinates are outside their valid range")
    return round(lat, COORD_DECIMALS), round(lon, COORD_DECIMALS)


def _google_json(value) -> str:
    return json.dumps(value, separators=(",", ":"), sort_keys=True, allow_nan=False)


def _stage_google_segment(conn: sqlite3.Connection, segment: dict, targets: dict) -> dict:
    """Stage GPS candidates and semantic intervals in connection-local TEMP tables."""
    counts = {"points_read": 0, "visits_read": 0, "activities_read": 0}

    def point(time: datetime, location, kind: str, priority: int) -> None:
        if location is None and kind != "timelinePath":
            return
        lat, lon = _google_coordinates(location)
        attrs = _google_json(
            {
                "latitude": lat,
                "longitude": lon,
                "source_type": "gps",
                "source": "google_timeline",
                "google_timeline_type": kind,
            }
        )
        conn.execute(
            "INSERT INTO google_points VALUES (?, ?, ?, ?, ?)"
            " ON CONFLICT(ts) DO UPDATE SET lat=excluded.lat, lon=excluded.lon,"
            " attributes=excluded.attributes, priority=excluded.priority"
            " WHERE excluded.priority > google_points.priority",
            (time.timestamp(), lat, lon, attrs, priority),
        )
        counts["points_read"] += 1

    for item in segment.get("timelinePath", []):
        if "time" in item:
            time = _google_time(item["time"])
        else:
            time = _google_time(segment["startTime"]) + timedelta(
                minutes=float(item["durationMinutesOffsetFromStartTime"])
            )
        point(time, item["point"], "timelinePath", 3)

    for kind, field in (("visit", CONF_PLACES_ENTITY), ("activity", CONF_ACTIVITY_ENTITY)):
        if kind not in segment:
            continue
        if not targets[kind]:
            raise ValueError(
                f"The file contains {kind} data: configure {field} on the tracker"
                f" or supply {field} in the service call (no entity will be created)"
            )
        start, end = (_google_time(segment[key]) for key in ("startTime", "endTime"))
        if end < start:
            raise ValueError("endTime must not be before startTime")
        data = segment[kind]
        candidate = data.get("topCandidate", {})
        attrs = {
            "source": "google_timeline",
            "start_time": segment["startTime"],
            "end_time": segment["endTime"],
            # Retain Google's metadata (placeId, probability, distanceMeters, etc.).
            "google_timeline": {kind: data},
        }
        for key in ("startTimeTimezoneUtcOffsetMinutes", "endTimeTimezoneUtcOffsetMinutes"):
            if key in segment:
                attrs["google_timeline"][key] = segment[key]
        rank = 0
        if kind == "visit":
            semantic_type = candidate.get("semanticType", "UNKNOWN")
            name = candidate.get("placeName") or candidate.get("name")
            name = name or {"HOME": "Home", "WORK": "Work"}.get(semantic_type)
            state = name or "unknown"
            if name:
                attrs["place_name"] = name
            attrs["place_id"] = candidate.get("placeId")
            attrs["google_semantic_type"] = semantic_type
            location = candidate.get("placeLocation")
            if location is not None:
                attrs["latitude"], attrs["longitude"] = _google_coordinates(location)
            point(start, location, kind, 1)
            point(end, location, kind, 1)
            rank = int(data.get("hierarchyLevel", 0))
            counts["visits_read"] += 1
        else:
            google_type = candidate.get("type", "UNKNOWN_ACTIVITY_TYPE")
            state = {
                "IN_VEHICLE": "automotive",
                "IN_PASSENGER_VEHICLE": "automotive",
                "WALKING": "walking",
                "RUNNING": "running",
                "CYCLING": "cycling",
                "ON_BICYCLE": "cycling",
                "STILL": "stationary",
                "UNKNOWN_ACTIVITY_TYPE": "unknown",
            }.get(google_type, google_type.lower())
            point(start, data.get("start"), kind, 2)
            point(end, data.get("end"), kind, 2)
            counts["activities_read"] += 1
        if not isinstance(state, str):
            raise ValueError("Place names and activity types must be strings")
        if end > start:
            conn.execute(
                "INSERT OR IGNORE INTO google_intervals"
                " (kind, start, end, state, attributes, rank) VALUES (?, ?, ?, ?, ?, ?)",
                (kind, start.timestamp(), end.timestamp(), state, _google_json(attrs), rank),
            )
    return counts


def _stage_google_states(conn: sqlite3.Connection, targets: dict) -> None:
    """Flatten intervals into upstream state history, including explicit gap/end states."""
    events = conn.execute(
        "SELECT start AS ts, 1 AS opening, id, kind, start, rank, state, attributes"
        " FROM google_intervals UNION ALL"
        " SELECT end, 0, id, kind, start, rank, state, attributes FROM google_intervals"
        " ORDER BY ts, opening, id"
    )
    active: dict[str, dict] = {"visit": {}, "activity": {}}
    previous: dict[str, tuple] = {}
    for ts, group in groupby(events, key=lambda row: row[0]):
        changed = set()
        for _, opening, identity, kind, start, rank, state, attrs in group:
            changed.add(kind)
            if opening:
                # Primary visits precede nested places; otherwise latest start wins.
                active[kind][identity] = (rank, -start, identity, state, attrs)
            else:
                active[kind].pop(identity, None)
        for kind in changed:
            selected = min(active[kind].values(), default=None)
            value = selected[-2:] if selected else ("unknown", '{"source":"google_timeline"}')
            if len(active[kind]) > 1:
                # Upstream displays one state at a time. Keep the other source
                # intervals in attributes instead of discarding their metadata.
                attrs = json.loads(value[1])
                attrs["google_overlapping_intervals"] = [
                    json.loads(other[-1])
                    for other in sorted(active[kind].values())
                    if other != selected
                ]
                value = (value[0], _google_json(attrs))
            if previous.get(kind) != value:
                conn.execute(
                    "INSERT INTO google_states VALUES (?, ?, ?, ?, ?)",
                    (targets[kind], ts, *value, kind),
                )
                previous[kind] = value


def _import_google_file(
    db_path: Path,
    file_name: str,
    entry_id: str,
    entity_id: str,
    targets: dict,
    time_zone: str,
    cancelled: threading.Event,
) -> dict:
    """Validate/stage off the event loop, then insert atomically into the original schema."""
    if (
        not file_name
        or Path(file_name).name != file_name
        or "\\" in file_name
        or Path(file_name).suffix.lower() != ".json"
    ):
        raise ValueError("file_name must be a JSON filename, without a directory")
    source = (db_path.parent / file_name).resolve(strict=True)
    if source.parent != db_path.parent.resolve() or not source.is_file():
        raise ValueError("The JSON file must be in the same directory as the database")

    result = {"points_read": 0, "visits_read": 0, "activities_read": 0, "segments_read": 0}
    # mode=rw never creates a missing archive. TEMP tables disappear on close and
    # can spill to disk; neither the database schema nor user_version is changed.
    with contextlib.closing(
        sqlite3.connect(f"{db_path.resolve().as_uri()}?mode=rw", uri=True, timeout=30)
    ) as conn:
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA temp_store=FILE")
        conn.set_progress_handler(lambda: int(cancelled.is_set()), 1000)
        if conn.execute("PRAGMA user_version").fetchone()[0] != SCHEMA_VERSION:
            raise ValueError("The archive must use the upstream database schema")
        conn.executescript(
            "CREATE TEMP TABLE google_points (ts REAL PRIMARY KEY, lat REAL, lon REAL,"
            " attributes TEXT, priority INTEGER);"
            "CREATE TEMP TABLE google_intervals (id INTEGER PRIMARY KEY, kind TEXT,"
            " start REAL, end REAL, state TEXT, attributes TEXT, rank INTEGER,"
            " UNIQUE(kind, start, end, state, attributes));"
            "CREATE TEMP TABLE google_states (entity_id TEXT, ts REAL, state TEXT,"
            " attributes TEXT, kind TEXT, PRIMARY KEY(entity_id, ts));"
        )
        with source.open(encoding="utf-8-sig") as stream:
            for index, segment in enumerate(
                _GoogleJSONReader(stream, cancelled).segments(), 1
            ):
                if cancelled.is_set():
                    raise ValueError("Import cancelled")
                try:
                    if not isinstance(segment, dict):
                        raise ValueError("Expected a segment object")
                    counts = _stage_google_segment(conn, segment, targets)
                except (AttributeError, KeyError, TypeError, ValueError, OverflowError) as err:
                    raise ValueError(f"Invalid semantic segment {index}: {err}") from err
                result["segments_read"] += 1
                for key, count in counts.items():
                    result[key] += count
        _stage_google_states(conn, targets)
        conn.commit()  # Only TEMP data has been written so far.
        zone = ZoneInfo(time_zone)
        days: set[str] = set()
        with conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT id FROM trackers WHERE entry_id=? AND entity_id=?",
                (entry_id, entity_id),
            ).fetchone()
            if row is None:
                raise ValueError("The configured tracker is no longer present in the archive")
            tracker_id = row[0]
            for target in targets.values():
                if (
                    target
                    and conn.execute(
                        "SELECT 1 FROM entity_states"
                        " WHERE entity_id=? AND tracker_id<>? LIMIT 1",
                        (target, tracker_id),
                    ).fetchone()
                ):
                    raise ValueError(f"{target} has archived history owned by another tracker")
            # Count only dates with new records, in Home Assistant's timezone.
            new_times = conn.execute(
                "SELECT ts FROM google_points AS g WHERE NOT EXISTS"
                " (SELECT 1 FROM points AS p WHERE p.tracker_id=? AND p.ts=g.ts)"
                " UNION ALL SELECT ts FROM google_states AS g WHERE NOT EXISTS"
                " (SELECT 1 FROM entity_states AS s"
                " WHERE s.entity_id=g.entity_id AND s.ts=g.ts)",
                (tracker_id,),
            )
            for (ts,) in new_times:
                days.add(datetime.fromtimestamp(ts, zone).date().isoformat())
            result["points_inserted"] = conn.execute(
                "INSERT OR IGNORE INTO points"
                " (tracker_id, ts, state, lat, lon, source_type, attributes)"
                " SELECT ?, ts, 'not_home', lat, lon, 'gps', attributes FROM google_points",
                (tracker_id,),
            ).rowcount
            for kind in targets:
                result[f"{kind}_states_inserted"] = conn.execute(
                    "INSERT OR IGNORE INTO entity_states"
                    " (tracker_id, entity_id, ts, state, attributes)"
                    " SELECT ?, entity_id, ts, state, attributes"
                    " FROM google_states WHERE kind=?",
                    (tracker_id, kind),
                ).rowcount
            if cancelled.is_set():
                raise ValueError("Import cancelled")
        result["days"] = len(days)
        result["points_skipped"] = result["points_read"] - result["points_inserted"]
        result["message"] = f"Imported records on {len(days)} days"
        return result
