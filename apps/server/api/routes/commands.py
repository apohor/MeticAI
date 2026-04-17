"""Machine command endpoints — publish MQTT commands to the Meticulous machine.

Each endpoint publishes a message to the appropriate MQTT topic via the local
Mosquitto broker.  The frontend never connects to MQTT directly; the FastAPI
server is the single gateway.
"""

import logging
import os
import uuid

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from services.mqtt_service import get_mqtt_subscriber

router = APIRouter()
logger = logging.getLogger(__name__)

TEST_MODE = os.environ.get("TEST_MODE") == "true"

# ---------------------------------------------------------------------------
# MQTT publishing helper
# ---------------------------------------------------------------------------

MQTT_TOPIC_PREFIX = "meticulous_espresso/command/"


def _publish_command(topic: str, payload: str = "") -> bool:
    """Publish a single message to the local MQTT broker.

    Uses paho-mqtt's ``publish.single()`` (fire-and-forget).
    Returns True on success, False on failure.
    """
    if TEST_MODE:
        logger.info("TEST_MODE: would publish %s → %r", topic, payload)
        return True

    mqtt_host = os.environ.get("MQTT_HOST", "127.0.0.1")
    mqtt_port = int(os.environ.get("MQTT_PORT", "1883"))

    try:
        import paho.mqtt.publish as publish

        publish.single(
            topic,
            payload=payload or None,
            hostname=mqtt_host,
            port=mqtt_port,
            client_id=f"meticai-cmd-{uuid.uuid4().hex[:8]}",
            qos=1,
        )
        return True
    except Exception as exc:
        logger.error("MQTT publish failed for %s: %s", topic, exc)
        return False


# ---------------------------------------------------------------------------
# Precondition helpers
# ---------------------------------------------------------------------------


def _get_snapshot() -> dict:
    """Return the current MQTT sensor snapshot (may be empty)."""
    sub = get_mqtt_subscriber()
    return sub.get_snapshot()


def _require_connected(snapshot: dict) -> None:
    availability = snapshot.get("availability")
    connected = snapshot.get("connected")
    if availability == "offline" or connected is False:
        raise HTTPException(status_code=409, detail="Machine is offline")


def _require_idle(snapshot: dict) -> None:
    _require_connected(snapshot)
    if snapshot.get("brewing"):
        raise HTTPException(
            status_code=409, detail="Cannot perform action: a shot is running"
        )


def _require_brewing(snapshot: dict) -> None:
    _require_connected(snapshot)
    if not snapshot.get("brewing"):
        raise HTTPException(
            status_code=409, detail="No shot is currently running"
        )


def _do_publish(action: str, payload: str = "") -> dict:
    """Publish and return a standard response."""
    topic = f"{MQTT_TOPIC_PREFIX}{action}"
    ok = _publish_command(topic, payload)
    if not ok:
        raise HTTPException(
            status_code=503, detail="Failed to publish MQTT command"
        )
    return {"success": True, "status": "ok", "command": action}


# ---------------------------------------------------------------------------
# Request bodies
# ---------------------------------------------------------------------------


class LoadProfileRequest(BaseModel):
    name: str = Field(..., min_length=1, description="Profile name to load on the machine")


class BrightnessRequest(BaseModel):
    value: int = Field(..., ge=0, le=100, description="Brightness 0–100")


class SoundsRequest(BaseModel):
    enabled: bool = Field(..., description="Enable or disable sounds")


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@router.post("/api/machine/command/start")
async def command_start():
    """Start a shot (load & execute the active profile)."""
    snapshot = _get_snapshot()
    _require_idle(snapshot)
    return _do_publish("start_shot")


@router.post("/api/machine/command/stop")
async def command_stop():
    """Stop the plunger immediately mid-shot."""
    snapshot = _get_snapshot()
    _require_brewing(snapshot)
    return _do_publish("stop_shot")


@router.post("/api/machine/command/abort")
async def command_abort():
    """Abort the current shot or preheat and retract the plunger."""
    snapshot = _get_snapshot()
    _require_connected(snapshot)
    # Abort is allowed during brewing OR preheating
    return _do_publish("abort_shot")


@router.post("/api/machine/command/continue")
async def command_continue():
    """Resume a paused shot."""
    return _do_publish("continue_shot")


@router.post("/api/machine/command/preheat")
async def command_preheat():
    """Preheat the water in the chamber.

    Also acts as a toggle: sending preheat while already preheating
    will cancel the preheat cycle.
    """
    snapshot = _get_snapshot()
    _require_connected(snapshot)
    # Allow during idle (start preheat) AND preheating (cancel preheat)
    state = (snapshot.get("state") or "").lower()
    if snapshot.get("brewing"):
        raise HTTPException(
            status_code=409, detail="Cannot preheat while brewing"
        )
    if state not in ("idle", "preheating", "heating", "click to start"):
        raise HTTPException(
            status_code=409,
            detail=f"Cannot preheat in current state: {snapshot.get('state')}",
        )
    return _do_publish("preheat")


@router.post("/api/machine/command/tare")
async def command_tare():
    """Zero the scale."""
    snapshot = _get_snapshot()
    _require_connected(snapshot)
    return _do_publish("tare_scale")


@router.post("/api/machine/command/home-plunger")
async def command_home_plunger():
    """Reset plunger to home position."""
    snapshot = _get_snapshot()
    _require_idle(snapshot)
    return _do_publish("home_plunger")


@router.post("/api/machine/command/purge")
async def command_purge():
    """Flush water through the group head."""
    snapshot = _get_snapshot()
    _require_idle(snapshot)
    return _do_publish("purge")


@router.post("/api/machine/command/load-profile")
async def command_load_profile(body: LoadProfileRequest):
    """Switch the machine to a different profile.

    Sends select_profile to highlight/load the profile on the machine's screen.
    Starting extraction is a separate explicit action via /start.
    """
    snapshot = _get_snapshot()
    _require_connected(snapshot)
    return _do_publish("select_profile", body.name)


@router.post("/api/machine/command/brightness")
async def command_brightness(body: BrightnessRequest):
    """Adjust the display brightness (0–100)."""
    snapshot = _get_snapshot()
    _require_connected(snapshot)
    return _do_publish("set_brightness", str(body.value))


@router.post("/api/machine/command/sounds")
async def command_sounds(body: SoundsRequest):
    """Toggle machine sound effects."""
    snapshot = _get_snapshot()
    _require_connected(snapshot)
    return _do_publish("enable_sounds", str(body.enabled).lower())


# ---------------------------------------------------------------------------
# Direct HTTP settings (not wired through the MQTT bridge)
# ---------------------------------------------------------------------------

# Whitelist of settings fields that ``POST /api/machine/settings`` accepts.
# Each entry maps the JSON key to a (type, validator) pair. The validator
# may raise ValueError to reject the value.
_ALLOWED_SETTINGS: dict[str, type] = {
    "heating_timeout": int,
    "update_channel": str,
    "hostname_override": str,  # None also allowed below
}


class SettingsPatch(BaseModel):
    heating_timeout: int | None = Field(
        None, ge=1, le=120,
        description="Heating timeout in minutes (1–120).",
    )
    update_channel: str | None = Field(
        None, min_length=1, max_length=32,
        description="Firmware update channel (e.g. stable, beta, nightly).",
    )
    # Empty string means 'clear the override' → we forward it as None.
    hostname_override: str | None = Field(
        None, max_length=63,
        description="Optional machine hostname override. Empty string clears it.",
    )


@router.get("/api/machine/settings")
async def get_machine_settings():
    """Return the machine's settings blob from its HTTP API.

    Used by the UI to populate fields that are not exposed via MQTT
    (e.g. ``heating_timeout``, ``update_channel``, ``hostname_override``).
    """
    snapshot = _get_snapshot()
    _require_connected(snapshot)
    from services.meticulous_service import (
        _get_http_client,
        _resolve_meticulous_base_url,
    )
    base_url = _resolve_meticulous_base_url()
    try:
        res = await _get_http_client().get(f"{base_url}/api/v1/settings", timeout=5.0)
        res.raise_for_status()
        return res.json()
    except Exception as exc:
        logger.error("Failed to fetch machine settings: %s", exc)
        raise HTTPException(status_code=502, detail="Failed to reach machine")


@router.post("/api/machine/settings")
async def patch_machine_settings(body: SettingsPatch):
    """Update a whitelisted subset of machine settings.

    Posts directly to the machine's HTTP API because these settings are
    not exposed over the MQTT bridge.
    """
    snapshot = _get_snapshot()
    _require_connected(snapshot)

    payload: dict = {}
    # Only include keys the client actually provided.
    data = body.model_dump(exclude_unset=True)
    for key, value in data.items():
        if key not in _ALLOWED_SETTINGS:
            raise HTTPException(status_code=400, detail=f"Unknown setting: {key}")
        if key == "hostname_override" and value == "":
            value = None
        payload[key] = value

    if not payload:
        raise HTTPException(status_code=400, detail="No settings provided")

    from services.meticulous_service import (
        _get_http_client,
        _resolve_meticulous_base_url,
    )
    base_url = _resolve_meticulous_base_url()
    try:
        res = await _get_http_client().post(
            f"{base_url}/api/v1/settings", json=payload, timeout=5.0,
        )
        res.raise_for_status()
    except Exception as exc:
        logger.error("Failed to patch settings %s: %s", list(payload), exc)
        raise HTTPException(status_code=502, detail="Failed to reach machine")
    return {"success": True, "status": "ok", "updated": payload}


# ---------------------------------------------------------------------------
# Machine discovery
# ---------------------------------------------------------------------------


@router.post("/api/machine/detect")
async def detect_machine():
    """
    Auto-detect Meticulous machine on the local network.
    
    Uses mDNS/Zeroconf and hostname resolution to find machines.
    
    Returns:
        - found: bool
        - ip: str (if found)
        - hostname: str (if found)
        - method: str (mdns | hostname)
        - guidance: str (if not found)
    """
    from services.machine_discovery_service import discover_machine, verify_machine
    
    result = await discover_machine()
    
    response = {
        "found": result.found,
    }
    
    if result.found:
        # Verify the machine is actually responding
        verified = await verify_machine(result.ip)
        response.update({
            "ip": result.ip,
            "hostname": result.hostname,
            "method": result.method,
            "verified": verified,
        })
    else:
        response["guidance"] = result.guidance
        response["guidance_key"] = result.guidance_key
        response["guidance_hints"] = result.guidance_hints
    
    return response
