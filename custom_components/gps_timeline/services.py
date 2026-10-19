from __future__ import annotations

import asyncio
from collections import Counter, defaultdict
from collections.abc import Iterator
import contextlib
from datetime import UTC, datetime, timedelta
import functools
import gzip
import io
from itertools import groupby
import json
import logging
import math
from pathlib import Path
import sqlite3
import tempfile
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
    db_path = Path(hass.config.path(DB_DIR_NAME, DB_FILE_NAME))
    audit = None
    creation = hass.async_add_executor_job(_GoogleImportLog, db_path.parent, dict(call.data))
    try:
        # Keep the worker reachable if cancellation happens while creating the log.
        await asyncio.wait([creation])
        audit = creation.result()
        return await _async_run_google_import(call, db_path, audit)
    except asyncio.CancelledError:
        if audit is None:
            audit = await creation
        await hass.async_add_executor_job(audit.finish, "Service cancelled")
        _LOGGER.warning("Google Timeline import cancelled. Diagnostic log: %s", audit.path)
        raise
    except Exception as err:
        if audit is None:
            raise HomeAssistantError(
                f"Cannot create Google Timeline import log: {err}"
            ) from err
        await hass.async_add_executor_job(audit.finish, str(err))
        _LOGGER.warning("Google Timeline import failed. Diagnostic log: %s", audit.path)
        raise HomeAssistantError(
            f"Google Timeline import failed: {err}. Diagnostic log: {audit.path}"
        ) from err


async def _async_run_google_import(
    call: ServiceCall, db_path: Path, audit: _GoogleImportLog
) -> dict:
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
        db_path,
        call.data["file_name"],
        entry.entry_id,
        entity_id,
        targets,
        hass.config.time_zone,
        cancelled,
        audit,
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
    _LOGGER.info("Google Timeline import for %s: %s", entity_id, result)
    return result


def _google_json_path(parent: str, key: str) -> str:
    """Keep even unusual/escaped source keys unambiguous."""
    if key.isidentifier():
        return f"{parent}.{key}"
    return f"{parent}[{json.dumps(key, ensure_ascii=True)}]"


class _GoogleJSONObject(dict):
    """Retain shadowed object members so diagnostics do not silently lose them."""

    __slots__ = ("shadowed",)

    def __init__(self, pairs) -> None:
        super().__init__(pairs)
        self.shadowed = []
        if len(self) != len(pairs):
            seen = {}
            for key, value in pairs:
                if key in seen:
                    self.shadowed.append((key, seen[key]))
                seen[key] = value


class _GoogleImportLog:
    """One private, unsampled JSON Lines audit beside the database per service call."""

    def __init__(self, folder: Path, request: dict) -> None:
        folder.mkdir(parents=True, exist_ok=True)
        stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%S.%fZ")
        # NamedTemporaryFile creates a unique file with permissions 0600.
        with contextlib.ExitStack() as files:
            self.raw = files.enter_context(
                tempfile.NamedTemporaryFile(
                    mode="wb",
                    prefix=f"google_timeline_import_{stamp}_",
                    suffix=".jsonl.gz",
                    dir=folder,
                    delete=False,
                )
            )
            self.path = Path(self.raw.name)
            self.stream = files.enter_context(
                io.TextIOWrapper(
                    gzip.GzipFile(filename="", mode="wb", fileobj=self.raw, compresslevel=3),
                    encoding="utf-8",
                )
            )
            self.counts: Counter = Counter()
            self.reasons: Counter = Counter()
            self.inventory: dict[str, Counter] = defaultdict(Counter)
            self.parse_complete = False
            self.committed = False
            self.blocking_errors: set[str] = set()
            self.result: dict = {}
            self.cancelled: threading.Event | None = None
            self.write(
                {
                    "event": "header",
                    "log_format_version": 1,
                    "importer_version": 2,
                    "started_at": datetime.now(UTC).isoformat(),
                    "request": request,
                    "notes": [
                        "Source values are complete, without sampling or truncation.",
                        "JSON paths use zero-based indices; inventory uses [*] for arrays.",
                        "Coordinates/UTC timestamps use upstream precision.",
                        "Entire visit/activity objects are kept as state metadata.",
                        "Unknown fields are logged even if a sibling field is imported.",
                        "Only a summary with committed=true confirms changes to the archive.",
                    ],
                }
            )
            self.stream.flush()
            self.files = files.pop_all()

    def write(self, value: dict) -> None:
        # ASCII escaping also makes arbitrary escaped Unicode safe to log losslessly.
        self.stream.write(json.dumps(value, separators=(",", ":"), allow_nan=False))
        self.stream.write("\n")

    def item(self, category: str, code: str, path: str, data, **details) -> None:
        self.write(
            {
                "event": "item",
                "category": category,
                "code": code,
                "json_path": path,
                "data": data,
                **details,
            }
        )
        self.counts[category] += 1
        self.reasons[code] += 1

    def observe(self, value, path: str, pattern: str) -> None:
        """Inventory all nested fields, including those kept only as raw metadata."""
        self.inventory[pattern][
            "object"
            if isinstance(value, dict)
            else "array"
            if isinstance(value, list)
            else type(value).__name__
        ] += 1
        if isinstance(value, dict):
            for key, discarded in getattr(value, "shadowed", []):
                self.item(
                    "not_imported",
                    "duplicate_json_key",
                    _google_json_path(path, key),
                    discarded,
                    reason="A later occurrence of this key wins",
                )
                self.observe(
                    discarded, _google_json_path(path, key), _google_json_path(pattern, key)
                )
            for key, child in value.items():
                self.observe(
                    child, _google_json_path(path, key), _google_json_path(pattern, key)
                )
        elif isinstance(value, list):
            for index, child in enumerate(value):
                self.observe(child, f"{path}[{index}]", f"{pattern}[*]")

    def finish(self, error: str = "") -> None:
        if self.stream.closed:
            return
        if not self.committed:
            for key in (
                "points_inserted",
                "visit_states_inserted",
                "activity_states_inserted",
                "days",
            ):
                self.result[key] = 0
        self.result.update(
            {
                "log_file": str(self.path),
                "parse_complete": self.parse_complete,
                "invalid_items": self.counts["invalid"],
                "unsupported_items": self.counts["unsupported"],
                "diagnostic_counts": dict(self.counts),
            }
        )
        status = "completed" if self.committed else "failed"
        if error == "Service cancelled" or (
            self.cancelled is not None and self.cancelled.is_set()
        ):
            status = "cancelled"
        try:
            self.write(
                {
                    "event": "summary",
                    "status": status,
                    "committed": self.committed,
                    "parse_complete": self.parse_complete,
                    "error": error or None,
                    "finished_at": datetime.now(UTC).isoformat(),
                    "result": self.result,
                    "counts_by_reason": dict(self.reasons),
                    "field_inventory": dict(self.inventory),
                }
            )
        finally:
            self.files.close()


class _GoogleJSONReader:
    """Decode one array item at a time, including exports larger than available RAM."""

    def __init__(
        self,
        stream: TextIO,
        cancelled: threading.Event | None = None,
        audit: _GoogleImportLog | None = None,
    ) -> None:
        self.stream = stream
        self.cancelled = cancelled
        self.audit = audit
        self.path = "$"
        self.offset = 0
        self.buffer = ""
        self.pos = 0
        self.eof = False
        self.decoder = json.JSONDecoder(
            parse_constant=self._invalid_constant,
            object_pairs_hook=_GoogleJSONObject,
            parse_float=self._finite_float,
        )

    @staticmethod
    def _finite_float(value: str) -> float:
        number = float(value)
        if not math.isfinite(number):
            raise ValueError("JSON number exceeds supported finite range")
        return number

    @staticmethod
    def _invalid_constant(value: str):
        raise ValueError(f"Invalid JSON number: {value}")

    def _read(self) -> None:
        if self.cancelled is not None and self.cancelled.is_set():
            raise ValueError("Import cancelled")
        chunk = self.stream.read(64 * 1024)
        self.offset += self.pos
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
            except json.JSONDecodeError as err:
                if self.eof:
                    raise ValueError(
                        f"Invalid JSON at character {self.offset + err.pos}: {err.msg}"
                    ) from None
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

    def array(self, path: str = "$") -> Iterator:
        self.take("[")
        index = 0
        if self.peek() != "]":
            while True:
                self.path = f"{path}[{index}]"
                yield self.value()
                index += 1
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
                    if self.audit:
                        self.audit.item(
                            "invalid",
                            "invalid_top_level_key",
                            "$",
                            key,
                            reason="Expected a unique top-level string key",
                        )
                    raise ValueError("Invalid or duplicate top-level JSON key")
                keys.add(key)
                self.take(":")
                self.path = path = _google_json_path("$", key)
                if key == "semanticSegments":
                    found = True
                    if self.audit:
                        self.audit.inventory[path]["array"] += 1
                    yield from self.array(path)
                elif self.peek() == "[":
                    if self.audit:
                        self.audit.inventory[path]["array"] += 1
                    length = 0
                    for index, value in enumerate(self.array(path)):
                        length += 1
                        if self.audit:
                            item_path = f"{path}[{index}]"
                            self.audit.observe(value, item_path, f"{path}[*]")
                            self.audit.item(
                                "unsupported",
                                "unsupported_top_level_item",
                                item_path,
                                value,
                                reason="No importer for this section",
                            )
                    if not length and self.audit:
                        self.audit.item("unsupported", "unsupported_top_level_field", path, [])
                else:
                    value = self.value()
                    if self.audit:
                        self.audit.observe(value, path, path)
                        self.audit.item(
                            "unsupported",
                            "unsupported_top_level_field",
                            path,
                            value,
                            reason="No importer for this section; entire subtree logged",
                        )
                if self.peek() == "}":
                    break
                self.take(",")
        self.path = "$"
        self.take("}")
        if self.peek():
            raise ValueError("Expected one JSON object containing semanticSegments")
        if self.audit:
            self.audit.parse_complete = True
        if not found:
            raise ValueError("Expected one JSON object containing semanticSegments")

    def log_unread(self, reason: str) -> None:
        """Preserve the exact unparsed text after syntax failure/cancellation in chunks."""
        if self.audit is None:
            return
        offset = self.offset + self.pos
        remaining = self.buffer[self.pos :]
        for index in range(0, len(remaining), 64 * 1024):
            chunk = remaining[index : index + 64 * 1024]
            self.audit.item(
                "not_imported",
                "unparsed_json_text",
                self.path,
                chunk,
                character_offset=offset,
                reason=reason,
            )
            offset += len(chunk)
        while chunk := self.stream.read(64 * 1024):
            self.audit.item(
                "not_imported",
                "unparsed_json_text",
                self.path,
                chunk,
                character_offset=offset,
                reason=reason,
            )
            offset += len(chunk)


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


def _stage_google_point(
    conn: sqlite3.Connection,
    time: datetime,
    coordinates: tuple,
    kind: str,
    priority: int,
    source: dict,
    audit: _GoogleImportLog,
) -> None:
    """Keep one point per timestamp and log the original value of every loser."""
    ts = time.timestamp()
    lat, lon = coordinates
    previous = conn.execute(
        "SELECT lat, lon, priority, source FROM google_points WHERE ts=?", (ts,)
    ).fetchone()
    if previous:
        old_lat, old_lon, old_priority, old_source = previous
        discarded = json.loads(old_source) if priority > old_priority else source
        kept = source if priority > old_priority else json.loads(old_source)
        audit.item(
            "duplicate",
            "duplicate_point"
            if (lat, lon) == (old_lat, old_lon)
            else "point_timestamp_collision",
            **discarded,
            reason="One coordinate per timestamp; path > activity > visit, first wins ties",
            timestamp=ts,
            kept_json_path=kept["path"],
        )
        if priority <= old_priority:
            return
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
        "INSERT INTO google_points VALUES (?, ?, ?, ?, ?, ?)"
        " ON CONFLICT(ts) DO UPDATE SET lat=excluded.lat, lon=excluded.lon,"
        " attributes=excluded.attributes, priority=excluded.priority, source=excluded.source",
        (ts, lat, lon, attrs, priority, _google_json(source)),
    )


def _stage_google_segment(
    conn: sqlite3.Connection,
    segment,
    targets: dict,
    audit: _GoogleImportLog,
    path: str,
) -> dict:
    """Validate independent source items, logging all unused fields and invalid siblings."""
    counts = {"points_read": 0, "visits_read": 0, "activities_read": 0}
    if not isinstance(segment, dict):
        audit.item(
            "invalid", "invalid_segment", path, segment, reason="Expected a segment object"
        )
        return counts
    common = {
        key: value
        for key, value in segment.items()
        if key
        in (
            "startTime",
            "endTime",
            "startTimeTimezoneUtcOffsetMinutes",
            "endTimeTimezoneUtcOffsetMinutes",
        )
    }
    known = {"timelinePath", "visit", "activity", *common}
    for key, value in segment.items():
        if key not in known:
            audit.item(
                "unsupported",
                "unsupported_segment_field",
                _google_json_path(path, key),
                value,
                context=common,
                reason="No mapping for this segment field; entire subtree logged",
            )
    if not any(key in segment for key in ("timelinePath", "visit", "activity")):
        audit.item(
            "unsupported",
            "unsupported_segment",
            path,
            segment,
            reason="No timelinePath, visit or activity",
        )
        return counts

    if "timelinePath" in segment:
        items = segment["timelinePath"]
        if not isinstance(items, list):
            audit.item(
                "invalid",
                "invalid_timeline_path",
                f"{path}.timelinePath",
                items,
                context=common,
                reason="Expected an array",
            )
            items = []
        elif not items:
            audit.item("not_imported", "empty_timeline_path", f"{path}.timelinePath", items)
        for index, item in enumerate(items):
            item_path = f"{path}.timelinePath[{index}]"
            source = {"path": item_path, "data": item, "context": common}
            try:
                if not isinstance(item, dict):
                    raise ValueError("Expected a point object")
                if "time" in item:
                    time = _google_time(item["time"])
                    used = {"point", "time"}
                else:
                    time = _google_time(segment["startTime"]) + timedelta(
                        minutes=float(item["durationMinutesOffsetFromStartTime"])
                    )
                    used = {"point", "durationMinutesOffsetFromStartTime"}
                coords = _google_coordinates(item["point"])
                # Validate before touching staging tables, so invalid points cannot leak in.
                time.timestamp()
            except (KeyError, TypeError, ValueError, OverflowError) as err:
                audit.item("invalid", "invalid_point", **source, reason=str(err))
                continue
            for key, value in item.items():
                if key not in used:
                    audit.item(
                        "unsupported",
                        "unmapped_point_field",
                        _google_json_path(item_path, key),
                        value,
                        context=common,
                        reason="Point imported, but this field is not archived",
                    )
            if isinstance(item["point"], dict):
                for key, value in item["point"].items():
                    if key != "latLng":
                        audit.item(
                            "unsupported",
                            "unmapped_coordinate_field",
                            _google_json_path(f"{item_path}.point", key),
                            value,
                        )
            _stage_google_point(conn, time, coords, "timelinePath", 3, source, audit)
            counts["points_read"] += 1

    archived_interval = False
    for kind, field in (("visit", CONF_PLACES_ENTITY), ("activity", CONF_ACTIVITY_ENTITY)):
        if kind not in segment:
            continue
        source = {"path": f"{path}.{kind}", "data": segment[kind], "context": common}
        if not targets[kind]:
            reason = f"Configure {field} on the tracker or supply {field} in the service call"
            audit.blocking_errors.add(reason)
            audit.item("not_imported", "missing_history_target", **source, reason=reason)
            continue
        try:
            start, end = (_google_time(segment[key]) for key in ("startTime", "endTime"))
            if end < start:
                raise ValueError("endTime must not be before startTime")
            data = segment[kind]
            if not isinstance(data, dict) or not isinstance(
                data.get("topCandidate", {}), dict
            ):
                raise ValueError(f"{kind} and topCandidate must be objects")
            candidate = data.get("topCandidate", {})
            attrs = {
                "source": "google_timeline",
                "start_time": segment["startTime"],
                "end_time": segment["endTime"],
                "google_timeline": {kind: data},
            }
            for key in (
                "startTimeTimezoneUtcOffsetMinutes",
                "endTimeTimezoneUtcOffsetMinutes",
            ):
                if key in segment:
                    attrs["google_timeline"][key] = segment[key]
            rank = 0
            locations = []
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
                    coords = _google_coordinates(location)
                    attrs["latitude"], attrs["longitude"] = coords
                    for time, role in ((start, "start"), (end, "end")):
                        locations.append(
                            (
                                time,
                                coords,
                                1,
                                {
                                    "path": f"{path}.visit.topCandidate.placeLocation",
                                    "data": location,
                                    "context": {**common, "coordinate_role": role},
                                },
                            )
                        )
                rank = int(data.get("hierarchyLevel", 0))
            else:
                google_type = candidate.get("type", "UNKNOWN_ACTIVITY_TYPE")
                if not isinstance(google_type, str):
                    raise ValueError("Activity type must be a string")
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
                for time, role in ((start, "start"), (end, "end")):
                    location = data.get(role)
                    if location is not None:
                        locations.append(
                            (
                                time,
                                _google_coordinates(location),
                                2,
                                {
                                    "path": f"{path}.activity.{role}",
                                    "data": location,
                                    "context": common,
                                },
                            )
                        )
            if not isinstance(state, str):
                raise ValueError("Place names must be strings")
            start_ts, end_ts = start.timestamp(), end.timestamp()
            serialized = _google_json(attrs)
        except (KeyError, TypeError, ValueError, OverflowError) as err:
            audit.item("invalid", f"invalid_{kind}", **source, reason=str(err))
            continue
        if end > start:
            cursor = conn.execute(
                "INSERT OR IGNORE INTO google_intervals"
                " (kind, start, end, state, attributes, rank, source)"
                " VALUES (?, ?, ?, ?, ?, ?, ?)",
                (kind, start_ts, end_ts, state, serialized, rank, _google_json(source)),
            )
            if not cursor.rowcount:
                audit.item(
                    "duplicate",
                    "duplicate_interval",
                    **source,
                    reason="Identical interval already present in this export",
                )
            archived_interval = True
        else:
            audit.item(
                "not_imported",
                "zero_duration_interval",
                **source,
                reason="Equal startTime/endTime: no state interval; GPS endpoints may import",
            )
        for time, coords, priority, point_source in locations:
            _stage_google_point(conn, time, coords, kind, priority, point_source, audit)
            counts["points_read"] += 1
        counts["visits_read" if kind == "visit" else "activities_read"] += 1
    if not archived_interval and common:
        audit.item(
            "not_imported",
            "segment_metadata_not_archived",
            path,
            common,
            reason="No archive row for these fields; path times may still use startTime",
        )
    return counts


def _stage_google_states(
    conn: sqlite3.Connection, targets: dict, cancelled: threading.Event
) -> None:
    """Flatten intervals into upstream state history, keeping provenance for audit."""
    events = conn.execute(
        "SELECT start AS ts, 1 AS opening, id, kind, start, rank, state, attributes, source"
        " FROM google_intervals UNION ALL"
        " SELECT end, 0, id, kind, start, rank, state, attributes, source"
        " FROM google_intervals"
        " ORDER BY ts, opening, id"
    )
    active: dict[str, dict] = {"visit": {}, "activity": {}}
    previous: dict[str, tuple] = {}
    for ts, group in groupby(events, key=lambda row: row[0]):
        if cancelled.is_set():
            raise ValueError("Import cancelled")
        changed = set()
        ended: dict[str, list] = defaultdict(list)
        for _, opening, identity, kind, start, rank, state, attrs, source in group:
            changed.add(kind)
            if opening:
                active[kind][identity] = (rank, -start, identity, state, attrs, source)
            else:
                active[kind].pop(identity, None)
                ended[kind].append(json.loads(source))
        for kind in changed:
            selected = min(active[kind].values(), default=None)
            value = selected[3:5] if selected else ("unknown", '{"source":"google_timeline"}')
            if len(active[kind]) > 1:
                attrs = json.loads(value[1])
                attrs["google_overlapping_intervals"] = [
                    json.loads(other[4])
                    for other in sorted(active[kind].values())
                    if other != selected
                ]
                value = (value[0], _google_json(attrs))
            if previous.get(kind) != value:
                sources = [json.loads(other[5]) for other in active[kind].values()]
                sources.extend(ended[kind])
                conn.execute(
                    "INSERT INTO google_states VALUES (?, ?, ?, ?, ?, ?)",
                    (targets[kind], ts, *value, kind, _google_json(sources)),
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
    audit: _GoogleImportLog | None = None,
) -> dict:
    """Own the diagnostic log through success, failure and cancellation."""
    audit = audit or _GoogleImportLog(
        db_path.parent,
        {
            "file_name": file_name,
            "entity_id": entity_id,
        },
    )
    audit.cancelled = cancelled
    try:
        audit.write({"event": "mapping", "history_targets": targets, "time_zone": time_zone})
        _import_google_archive(
            db_path, file_name, entry_id, entity_id, targets, time_zone, cancelled, audit
        )
    except Exception as err:
        audit.finish(str(err))
        raise
    audit.finish()
    return audit.result


def _import_google_archive(
    db_path: Path,
    file_name: str,
    entry_id: str,
    entity_id: str,
    targets: dict,
    time_zone: str,
    cancelled: threading.Event,
    audit: _GoogleImportLog,
) -> None:
    """Read the complete source, then insert valid records in one upstream transaction."""
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
    audit.write(
        {"event": "source", "file_name": file_name, "size_bytes": source.stat().st_size}
    )
    result = audit.result
    result.update(
        {"points_read": 0, "visits_read": 0, "activities_read": 0, "segments_read": 0}
    )
    with contextlib.closing(
        sqlite3.connect(f"{db_path.resolve().as_uri()}?mode=rw", uri=True, timeout=30)
    ) as conn:
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA temp_store=FILE")
        if conn.execute("PRAGMA user_version").fetchone()[0] != SCHEMA_VERSION:
            raise ValueError("The archive must use the upstream database schema")
        # Provenance is TEMP-only. It never changes the archive's schema or rows.
        conn.executescript(
            "CREATE TEMP TABLE google_points (ts REAL PRIMARY KEY, lat REAL, lon REAL,"
            " attributes TEXT, priority INTEGER, source TEXT);"
            "CREATE TEMP TABLE google_intervals (id INTEGER PRIMARY KEY, kind TEXT,"
            " start REAL, end REAL, state TEXT, attributes TEXT, rank INTEGER, source TEXT,"
            " UNIQUE(kind, start, end, state, attributes));"
            "CREATE TEMP TABLE google_states (entity_id TEXT, ts REAL, state TEXT,"
            " attributes TEXT, kind TEXT, source TEXT, PRIMARY KEY(entity_id, ts));"
        )
        try:
            with source.open(encoding="utf-8-sig") as stream:
                reader = _GoogleJSONReader(stream, cancelled, audit)
                try:
                    for index, segment in enumerate(reader.segments()):
                        path = f"$.semanticSegments[{index}]"
                        audit.observe(segment, path, "$.semanticSegments[*]")
                        if cancelled.is_set():
                            audit.item("not_imported", "cancelled_segment", path, segment)
                            raise ValueError("Import cancelled")
                        counts = _stage_google_segment(conn, segment, targets, audit, path)
                        result["segments_read"] += 1
                        for key, count in counts.items():
                            result[key] += count
                except (ValueError, OSError) as err:
                    audit.item(
                        "not_imported" if cancelled.is_set() else "invalid",
                        "read_cancelled" if cancelled.is_set() else "json_read_failed",
                        reader.path,
                        None,
                        reason=str(err),
                        character_offset=reader.offset + reader.pos,
                    )
                    reader.log_unread(str(err))
                    raise
            audit.parse_complete = True
            if audit.blocking_errors:
                raise ValueError("; ".join(sorted(audit.blocking_errors)))
            _stage_google_states(conn, targets, cancelled)
            conn.commit()  # Only TEMP data has been written so far.
            # Interrupting SQLite can roll back its transaction. Keep committed TEMP
            # provenance available so every accepted source can be logged on abort.
            conn.set_progress_handler(lambda: int(cancelled.is_set()), 1000)
            zone = ZoneInfo(time_zone)
            days: set[str] = set()
            with conn:
                conn.execute("BEGIN IMMEDIATE")
                row = conn.execute(
                    "SELECT id FROM trackers WHERE entry_id=? AND entity_id=?",
                    (entry_id, entity_id),
                ).fetchone()
                if row is None:
                    raise ValueError(
                        "The configured tracker is no longer present in the archive"
                    )
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
                        raise ValueError(
                            f"{target} has archived history owned by another tracker"
                        )
                for raw, ts, old_lat, old_lon in conn.execute(
                    "SELECT g.source, g.ts, p.lat, p.lon FROM google_points AS g"
                    " JOIN points AS p ON p.ts=g.ts AND p.tracker_id=?",
                    (tracker_id,),
                ):
                    audit.item(
                        "existing",
                        "existing_point",
                        **json.loads(raw),
                        timestamp=ts,
                        reason="Existing archive point is preserved",
                        existing_coordinates={"latitude": old_lat, "longitude": old_lon},
                    )
                for raw, ts, state, attrs, target, old_state in conn.execute(
                    "SELECT g.source, g.ts, g.state, g.attributes, g.entity_id, s.state"
                    " FROM google_states AS g JOIN entity_states AS s"
                    " ON s.entity_id=g.entity_id AND s.ts=g.ts"
                ):
                    sources = json.loads(raw)
                    audit.item(
                        "existing",
                        "existing_companion_state",
                        sources[0]["path"],
                        {"timestamp": ts, "state": state, "attributes": json.loads(attrs)},
                        source_items=sources,
                        entity_id=target,
                        existing_state=old_state,
                        reason="Existing state is preserved; source items listed in full",
                    )
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
                    " SELECT ?, ts, 'not_home', lat, lon, 'gps', attributes"
                    " FROM google_points",
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
                # All detail records must be writable before we commit the archive.
                audit.stream.flush()
                if cancelled.is_set():
                    raise ValueError("Import cancelled")
            audit.committed = True
            result["days"] = len(days)
            result["points_skipped"] = result["points_read"] - result["points_inserted"]
            result["message"] = (
                f"Imported records on {len(days)} days; diagnostic log: {audit.path.name}"
            )
        except Exception:
            # After rollback, every previously accepted source item still needs an
            # explicit 'not imported' explanation. TEMP provenance is retained here.
            conn.set_progress_handler(None, 0)
            for (raw,) in conn.execute(
                "SELECT source FROM google_points"
                " UNION ALL SELECT source FROM google_intervals"
            ):
                audit.item(
                    "not_imported",
                    "import_aborted",
                    **json.loads(raw),
                    reason="Import failed or was cancelled; no archive changes committed",
                )
            raise
