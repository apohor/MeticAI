"""Shared state, persistence, and helper functions for scheduling.

This module is the single source of truth for:
- Scheduled shots state and persistence
- Recurring schedules state and persistence  
- Helper functions for schedule timing calculations
"""
from datetime import datetime, timezone, timedelta
from typing import Optional
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
import logging
import json
import asyncio
from pathlib import Path

from config import DATA_DIR

logger = logging.getLogger(__name__)

# ==============================================================================
# Shared In-Memory State
# ==============================================================================
# These are the ONLY copies of these dictionaries - all modules should import from here

_scheduled_shots: dict = {}
_scheduled_tasks: dict = {}
_recurring_schedules: dict = {}

# Module-level lock protecting mutations to the above dicts.
# Callers that read-modify-write any of these dicts across an ``await``
# boundary MUST hold this lock to prevent interleaved mutations.
# Created lazily and recreated when the running event loop changes so that
# tests using a new loop per function don't hit "attached to a different loop".
_state_lock: Optional[asyncio.Lock] = None
_state_lock_loop: Optional[asyncio.AbstractEventLoop] = None


def _get_state_lock() -> asyncio.Lock:
    """Return the module-level state lock, (re)creating it when needed.
    
    The lock is recreated when the running event loop differs from the one
    it was originally bound to.  This avoids ``RuntimeError: ... attached to
    a different loop`` in test suites that create a fresh loop per test.
    """
    global _state_lock, _state_lock_loop
    try:
        running_loop = asyncio.get_running_loop()
    except RuntimeError:
        running_loop = None

    if _state_lock is None or (running_loop is not None and running_loop is not _state_lock_loop):
        _state_lock = asyncio.Lock()
        _state_lock_loop = running_loop
    return _state_lock

# Constant for preheat duration
PREHEAT_DURATION_MINUTES = 10


# ==============================================================================
# Machine Timezone Cache
# ==============================================================================
# Meticulous machines expose ``time_zone`` (IANA, e.g. "America/New_York") via
# ``GET /api/v1/settings``. We cache it so recurring schedules with no explicit
# timezone fall back to the machine's local time rather than UTC.

_machine_timezone_cache: Optional[str] = None


async def refresh_machine_timezone() -> Optional[str]:
    """Fetch and cache the machine's configured timezone.

    Reads ``time_zone`` from the Meticulous machine's settings. On error,
    the cache is left unchanged. Returns the cached value (new or existing)
    or ``None`` if the machine has never been reachable.
    """
    global _machine_timezone_cache
    try:
        # Deferred import to avoid a circular dependency between
        # scheduling_state -> meticulous_service -> settings_service.
        from services.meticulous_service import async_get_settings

        settings = await async_get_settings()
        if settings is None:
            return _machine_timezone_cache

        # pyMeticulous returns either a Pydantic model or a dict-like value.
        if hasattr(settings, "model_dump"):
            data = settings.model_dump()
        elif hasattr(settings, "__dict__"):
            data = dict(vars(settings))
        else:
            try:
                data = dict(settings)
            except Exception:
                data = {}

        tz = data.get("time_zone") or data.get("timezone")
        if isinstance(tz, str) and tz.strip():
            _machine_timezone_cache = tz.strip()
            logger.debug(f"Cached machine timezone: {_machine_timezone_cache}")
    except Exception as e:
        logger.debug(f"Could not refresh machine timezone: {e}")
    return _machine_timezone_cache


def get_cached_machine_timezone() -> Optional[str]:
    """Return the last known machine timezone (may be None if never fetched)."""
    return _machine_timezone_cache


# ==============================================================================
# Scheduled Shots Persistence
# ==============================================================================

class ScheduledShotsPersistence:
    """Manages persistence of scheduled shots to disk.
    
    Scheduled shots are stored in a JSON file to survive server restarts.
    This ensures that scheduled shots are not lost during crashes, deploys,
    or host reboots.
    """
    
    def __init__(self, persistence_file: str | Path | None = None):
        """Initialize the persistence layer.
        
        Args:
            persistence_file: Path to the JSON file for storing scheduled shots.
                             Defaults to DATA_DIR/scheduled_shots.json.
        """
        if persistence_file is None:
            self.persistence_file = DATA_DIR / "scheduled_shots.json"
        else:
            self.persistence_file = Path(persistence_file)
        self._lock = asyncio.Lock()
        
        # Ensure the parent directory exists
        self.persistence_file.parent.mkdir(parents=True, exist_ok=True)
    
    async def save(self, scheduled_shots: dict) -> None:
        """Save scheduled shots to disk.
        
        Args:
            scheduled_shots: Dictionary of scheduled shots to persist.
        """
        async with self._lock:
            try:
                # Only save shots that are scheduled or preheating (not completed/failed/cancelled)
                active_shots = {
                    shot_id: shot for shot_id, shot in scheduled_shots.items()
                    if shot.get("status") in ["scheduled", "preheating"]
                }
                
                # Write atomically using a temporary file
                temp_file = self.persistence_file.with_suffix('.tmp')
                with open(temp_file, 'w') as f:
                    json.dump(active_shots, f, indent=2)
                
                # Atomic rename
                temp_file.replace(self.persistence_file)
                
                logger.debug(f"Persisted {len(active_shots)} scheduled shots to {self.persistence_file}")
            except Exception as e:
                logger.error(f"Failed to save scheduled shots: {e}", exc_info=True)
    
    async def load(self) -> dict:
        """Load scheduled shots from disk.
        
        Returns:
            Dictionary of scheduled shots, or empty dict if file doesn't exist or is invalid.
        """
        async with self._lock:
            try:
                if not self.persistence_file.exists():
                    logger.info("No persisted scheduled shots found (first run)")
                    return {}
                
                with open(self.persistence_file, 'r') as f:
                    data = json.load(f)
                
                if not isinstance(data, dict):
                    logger.warning("Invalid scheduled shots file format, ignoring")
                    return {}
                
                logger.info(f"Loaded {len(data)} scheduled shots from {self.persistence_file}")
                return data
            except json.JSONDecodeError as e:
                logger.error(f"Corrupt scheduled shots file, ignoring: {e}")
                # Backup the corrupt file
                try:
                    backup_file = self.persistence_file.with_suffix('.corrupt')
                    self.persistence_file.rename(backup_file)
                    logger.info(f"Backed up corrupt file to {backup_file}")
                except Exception:
                    # Ignore errors during backup (file system issues, permissions, etc.)
                    pass
                return {}
            except Exception as e:
                logger.error(f"Failed to load scheduled shots: {e}", exc_info=True)
                return {}
    
    async def clear(self) -> None:
        """Clear all persisted scheduled shots."""
        async with self._lock:
            try:
                if self.persistence_file.exists():
                    self.persistence_file.unlink()
                    logger.info("Cleared persisted scheduled shots")
            except Exception as e:
                logger.error(f"Failed to clear scheduled shots: {e}", exc_info=True)


# ==============================================================================
# Recurring Schedules Persistence
# ==============================================================================

class RecurringSchedulesPersistence:
    """Manages persistence of recurring schedules to disk.
    
    Recurring schedules define repeated preheat/shot times (e.g., daily, weekdays).
    """
    
    def __init__(self, persistence_file: str | Path | None = None):
        if persistence_file is None:
            self.persistence_file = DATA_DIR / "recurring_schedules.json"
        else:
            self.persistence_file = Path(persistence_file)
        self._lock = asyncio.Lock()
        
        self.persistence_file.parent.mkdir(parents=True, exist_ok=True)
    
    async def save(self, schedules: dict) -> None:
        """Save recurring schedules to disk.
        
        Persists ALL schedules (enabled and disabled) so that disabled
        schedules survive restarts and can be re-enabled later.
        """
        async with self._lock:
            try:
                temp_file = self.persistence_file.with_suffix('.tmp')
                with open(temp_file, 'w') as f:
                    json.dump(schedules, f, indent=2)
                temp_file.replace(self.persistence_file)
                
                enabled_count = sum(1 for s in schedules.values() if s.get("enabled", True))
                logger.debug(f"Persisted {len(schedules)} recurring schedules ({enabled_count} enabled)")
            except Exception as e:
                logger.error(f"Failed to save recurring schedules: {e}", exc_info=True)
    
    async def load(self) -> dict:
        """Load recurring schedules from disk."""
        async with self._lock:
            try:
                if not self.persistence_file.exists():
                    return {}
                
                with open(self.persistence_file, 'r') as f:
                    data = json.load(f)
                
                if not isinstance(data, dict):
                    return {}
                
                logger.info(f"Loaded {len(data)} recurring schedules")
                return data
            except Exception as e:
                logger.error(f"Failed to load recurring schedules: {e}", exc_info=True)
                return {}


# ==============================================================================
# Persistence Instances and Helper Functions  
# ==============================================================================

# Initialize persistence layers
_scheduled_shots_persistence = ScheduledShotsPersistence()
_recurring_schedules_persistence = RecurringSchedulesPersistence()

# Legacy alias for backward compatibility
SchedulePersistence = ScheduledShotsPersistence


async def save_scheduled_shots():
    """Save scheduled shots to persistence."""
    await _scheduled_shots_persistence.save(_scheduled_shots)


async def load_scheduled_shots() -> dict:
    """Load scheduled shots from persistence."""
    return await _scheduled_shots_persistence.load()


async def save_recurring_schedules():
    """Save recurring schedules to persistence."""
    await _recurring_schedules_persistence.save(_recurring_schedules)


async def load_recurring_schedules():
    """Load recurring schedules from persistence.
    
    Note: We use clear()/update() to mutate the existing dict in place,
    ensuring any module that imported _recurring_schedules directly
    will see the loaded data.
    """
    loaded = await _recurring_schedules_persistence.load()
    async with _get_state_lock():
        _recurring_schedules.clear()
        _recurring_schedules.update(loaded)


async def restore_scheduled_shots():
    """Restore scheduled shots from disk on startup.
    
    Note: We use clear()/update() to mutate the existing dict in place,
    ensuring any module that imported _scheduled_shots directly
    will see the loaded data.
    """
    loaded = await _scheduled_shots_persistence.load()
    async with _get_state_lock():
        _scheduled_shots.clear()
        _scheduled_shots.update(loaded)
    
    if _scheduled_shots:
        logger.info(f"Restored {len(_scheduled_shots)} scheduled shots from persistence")


# ==============================================================================
# Schedule Timing Calculations
# ==============================================================================

def get_next_occurrence(schedule: dict) -> Optional[datetime]:
    """Calculate the next occurrence of a recurring schedule.
    
    Args:
        schedule: Recurring schedule dict with:
            - time: HH:MM format
            - timezone: IANA tz name (e.g. "America/Los_Angeles"). Defaults to UTC.
            - recurrence_type: 'daily', 'weekdays', 'weekends', 'interval', 'specific_days'
            - interval_days: For 'interval' type, number of days between runs
            - days_of_week: For 'specific_days' type, list of day numbers (0=Monday)
    
    Returns:
        Next datetime (UTC) when the schedule should run, or None if invalid.
    """
    MAX_SCHEDULING_DAYS = 400  # Maximum ~1 year ahead to search for next occurrence
    
    try:
        time_str = schedule.get("time", "07:00")
        hour, minute = map(int, time_str.split(":"))
        recurrence_type = schedule.get("recurrence_type", "daily")

        # Resolve schedule's timezone. Precedence:
        #   1. Explicit ``timezone`` on the schedule (user intent).
        #   2. Cached machine ``time_zone`` from /api/v1/settings.
        #   3. UTC fallback.
        tz_name = schedule.get("timezone") or _machine_timezone_cache or "UTC"
        try:
            tz = ZoneInfo(tz_name)
        except ZoneInfoNotFoundError:
            logger.warning(
                f"Unknown timezone '{tz_name}' for schedule {schedule.get('id')}, falling back to UTC"
            )
            tz = ZoneInfo("UTC")

        # Work in the schedule's local timezone so HH:MM means local wall-clock time.
        now_local = datetime.now(tz)
        today_local = now_local.date()

        # Start checking from today (in local tz)
        candidate = datetime(
            today_local.year, today_local.month, today_local.day,
            hour, minute, tzinfo=tz,
        )

        # If today's time has passed locally, start from tomorrow
        if candidate <= now_local:
            candidate += timedelta(days=1)
        
        # Find the next valid day based on recurrence type
        for _ in range(MAX_SCHEDULING_DAYS):
            weekday = candidate.weekday()  # 0=Monday, 6=Sunday
            
            if recurrence_type == "daily":
                return candidate.astimezone(timezone.utc)
            elif recurrence_type == "weekdays" and weekday < 5:  # Mon-Fri
                return candidate.astimezone(timezone.utc)
            elif recurrence_type == "weekends" and weekday >= 5:  # Sat-Sun
                return candidate.astimezone(timezone.utc)
            elif recurrence_type == "interval":
                interval_days = schedule.get("interval_days", 1)
                # Check if this day is valid based on last_run
                last_run = schedule.get("last_run")
                if last_run:
                    try:
                        # Handle ISO format with trailing Z (replace with +00:00 for fromisoformat)
                        last_run_str = last_run.replace('Z', '+00:00') if isinstance(last_run, str) else str(last_run)
                        last_run_dt = datetime.fromisoformat(last_run_str)
                        days_since = (candidate - last_run_dt).days
                        if days_since >= interval_days:
                            return candidate.astimezone(timezone.utc)
                    except (ValueError, AttributeError):
                        # Invalid datetime format - treat as no last run
                        logger.warning(f"Invalid last_run format for schedule {schedule.get('id')}: {last_run}")
                        return candidate.astimezone(timezone.utc)
                else:
                    # No last run, so this is the first run
                    return candidate.astimezone(timezone.utc)
            elif recurrence_type == "specific_days":
                days_of_week = schedule.get("days_of_week", [])
                if weekday in days_of_week:
                    return candidate.astimezone(timezone.utc)
            
            # Move to next day
            candidate += timedelta(days=1)
        
        return None
    except Exception as e:
        logger.error(f"Failed to calculate next occurrence: {e}")
        return None

