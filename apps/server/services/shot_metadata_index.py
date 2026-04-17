"""Persistent on-disk index of shot metadata.

When a shot file is fetched (expensive: HTTP GET + zstd decompress of the
full telemetry payload), we extract the handful of fields used to render
listings — profile name/id, timestamp, final weight, total time — and
persist them in ``DATA_DIR/shot_metadata_index.json``.

Subsequent listings can then serve the vast majority of shots from the
on-disk index without going back to the machine. Only shots that have
never been seen before trigger a full fetch.

The index is treated as an authoritative, append-only cache: entries are
only (re)written when we've freshly fetched a shot. They're cleared via
:func:`invalidate` when the user explicitly asks for a refresh or when
the underlying shot file is known to have changed.
"""

from __future__ import annotations

import asyncio
import json
import logging
import tempfile
from pathlib import Path
from typing import Any, Iterable, Optional

from config import DATA_DIR

logger = logging.getLogger(__name__)

_INDEX_FILE: Path = DATA_DIR / "shot_metadata_index.json"
_index: dict[str, dict[str, Any]] = {}
_loaded = False
_lock = asyncio.Lock()


def _key(date: str, filename: str) -> str:
    return f"{date}/{filename}"


def _load_from_disk_sync() -> None:
    """Load the index from disk. Safe to call multiple times."""
    global _index, _loaded
    if _loaded:
        return
    try:
        if _INDEX_FILE.exists():
            with open(_INDEX_FILE, "r") as f:
                raw = json.load(f)
            if isinstance(raw, dict):
                _index = {k: v for k, v in raw.items() if isinstance(v, dict)}
                logger.info(f"Loaded {len(_index)} shot metadata entries from {_INDEX_FILE}")
    except (OSError, json.JSONDecodeError) as e:
        logger.warning(f"Could not load shot metadata index: {e}; starting fresh")
        _index = {}
    _loaded = True


def _save_to_disk_sync() -> None:
    """Atomically persist the index to disk."""
    try:
        _INDEX_FILE.parent.mkdir(parents=True, exist_ok=True)
        # Write-then-rename for atomicity.
        with tempfile.NamedTemporaryFile(
            "w", delete=False, dir=str(_INDEX_FILE.parent), suffix=".tmp"
        ) as tmp:
            json.dump(_index, tmp, separators=(",", ":"))
            tmp_path = Path(tmp.name)
        tmp_path.replace(_INDEX_FILE)
    except OSError as e:
        logger.warning(f"Could not persist shot metadata index: {e}")


def extract_metadata(date: str, filename: str, shot_data: dict) -> dict[str, Any]:
    """Extract the lightweight listing fields from a full shot-data payload."""
    profile_name = shot_data.get("profile_name", "") or ""
    profile_id = ""
    profile_obj = shot_data.get("profile")
    if isinstance(profile_obj, dict):
        if not profile_name:
            profile_name = profile_obj.get("name", "") or ""
        profile_id = profile_obj.get("id", "") or ""

    data_entries = shot_data.get("data") or []
    final_weight: Optional[float] = None
    total_time_ms: Optional[float] = None
    if data_entries:
        last_entry = data_entries[-1]
        if isinstance(last_entry, dict):
            shot_block = last_entry.get("shot")
            if isinstance(shot_block, dict):
                final_weight = shot_block.get("weight")
            total_time_ms = last_entry.get("time")

    return {
        "profile_name": profile_name,
        "profile_id": profile_id,
        "timestamp": shot_data.get("time"),
        "final_weight": final_weight,
        "total_time": (total_time_ms / 1000) if total_time_ms else None,
    }


def ensure_loaded() -> None:
    """Load the index into memory if not yet loaded (sync, lock-free).

    Safe to call from both sync and async contexts; the file read itself is
    tiny (flat JSON, a few kilobytes).
    """
    if not _loaded:
        _load_from_disk_sync()


def get(date: str, filename: str) -> Optional[dict[str, Any]]:
    """Return cached metadata for ``(date, filename)``, or ``None`` if unknown."""
    ensure_loaded()
    return _index.get(_key(date, filename))


def get_many(pairs: Iterable[tuple[str, str]]) -> dict[tuple[str, str], dict[str, Any]]:
    """Return cached metadata for each known ``(date, filename)`` pair."""
    ensure_loaded()
    out: dict[tuple[str, str], dict[str, Any]] = {}
    for date, filename in pairs:
        entry = _index.get(_key(date, filename))
        if entry is not None:
            out[(date, filename)] = entry
    return out


async def put(date: str, filename: str, metadata: dict[str, Any]) -> None:
    """Upsert a single entry and persist to disk."""
    ensure_loaded()
    async with _lock:
        _index[_key(date, filename)] = metadata
        await asyncio.to_thread(_save_to_disk_sync)


async def put_many(entries: dict[tuple[str, str], dict[str, Any]]) -> None:
    """Upsert multiple entries and persist once."""
    if not entries:
        return
    ensure_loaded()
    async with _lock:
        for (date, filename), meta in entries.items():
            _index[_key(date, filename)] = meta
        await asyncio.to_thread(_save_to_disk_sync)


async def invalidate(date: Optional[str] = None, filename: Optional[str] = None) -> None:
    """Invalidate index entries.

    - No args → clear the whole index.
    - ``date`` only → drop every entry for that date.
    - ``date`` + ``filename`` → drop just that single entry.
    """
    ensure_loaded()
    async with _lock:
        if date is None:
            _index.clear()
        elif filename is None:
            prefix = f"{date}/"
            for k in [k for k in _index if k.startswith(prefix)]:
                del _index[k]
        else:
            _index.pop(_key(date, filename), None)
        await asyncio.to_thread(_save_to_disk_sync)


def size() -> int:
    ensure_loaded()
    return len(_index)
