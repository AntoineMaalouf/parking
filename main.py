import asyncio
import logging
import os
import re
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Optional

import httpx
from dotenv import load_dotenv

import db
from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

load_dotenv()

_data_dir = os.getenv("DATA_DIR", ".")
os.makedirs(_data_dir, exist_ok=True)

logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
api_log = logging.getLogger("melbourne_api")

# ── App state ─────────────────────────────────────────────────────────────────

SENSOR_API = (
    "https://data.melbourne.vic.gov.au/api/explore/v2.1/catalog/datasets"
    "/on-street-parking-bay-sensors/records"
)
SENSOR_EXPORT_API = (
    "https://data.melbourne.vic.gov.au/api/explore/v2.1/catalog/datasets"
    "/on-street-parking-bay-sensors/exports/json"
)
BAYS_EXPORT_API = (
    "https://data.melbourne.vic.gov.au/api/explore/v2.1/catalog/datasets"
    "/on-street-parking-bays/exports/json"
)
PUSHOVER_API = "https://api.pushover.net/1/messages.json"
PAGE_SIZE = 100

REQUEST_TIMEOUT = httpx.Timeout(connect=10, read=60, write=10, pool=5)

# {bay_id: {"street": str, "road_description": str}} — bays being watched
watched_bays: dict[int, dict] = {}

# Full bay data store: {kerbsideid: raw_record_dict}
_bay_store: dict[int, dict] = {}
# ISO timestamp of last successful sensor fetch (used as delta filter)
_last_sensor_fetch: str | None = None

# Street map cache (no TTLCache — managed manually with _street_map_fetched_at)
_street_map: dict[str, str] = {}
_street_map_fetched_at: float = 0.0
STREET_MAP_TTL = 86400  # 24 hours

# Parking response cache
_parking_cache: dict | None = None
_parking_cache_at: float = 0.0
PARKING_CACHE_TTL = 120  # 2 minutes

# Lock preventing concurrent refreshes from doubling API calls (#3)
_refresh_lock = asyncio.Lock()
_street_map_lock = asyncio.Lock()


@asynccontextmanager
async def lifespan(_: FastAPI):
    global _last_sensor_fetch  # declared at function top, not nested in an if (#1)

    await db.init_db()

    # Restore bay store from DB (avoids full API fetch on restart)
    stored = await db.load_bay_store()
    if stored:
        _bay_store.update(stored)
        _last_sensor_fetch = await db.get_state("last_sensor_fetch")

    # Restore watched bays from DB
    watched = await db.load_watched_bays()
    watched_bays.update(watched)

    asyncio.create_task(_watch_loop())
    yield


app = FastAPI(title="Melbourne Parking Map", lifespan=lifespan)


# ── Pushover ──────────────────────────────────────────────────────────────────

def pushover_configured() -> bool:
    return bool(os.getenv("PUSHOVER_API_TOKEN")) and bool(os.getenv("PUSHOVER_USER_KEY"))


async def send_pushover(title: str, message: str) -> bool:
    """Send a Pushover notification. Returns True on success. (#7)"""
    async with httpx.AsyncClient(timeout=10) as client:
        response = await client.post(PUSHOVER_API, data={
            "token": os.getenv("PUSHOVER_API_TOKEN"),
            "user":  os.getenv("PUSHOVER_USER_KEY"),
            "title": title,
            "message": message,
        })
    if response.status_code != 200:
        api_log.error("Pushover HTTP error %d: %s", response.status_code, response.text)
        return False
    body = response.json()
    if body.get("status") != 1:
        api_log.error("Pushover delivery failed: %s", body)
        return False
    return True


# ── Background watcher ────────────────────────────────────────────────────────

async def _watch_loop() -> None:
    """Every 2 minutes, refresh sensor data and notify for any watched bay that became free."""
    while True:
        await asyncio.sleep(120)
        if not watched_bays:
            continue
        try:
            async with _refresh_lock:
                await _refresh_sensors()
            freed = []
            for bay_id in list(watched_bays.keys()):
                record = _bay_store.get(bay_id)
                if record and record.get("status_description") == "Unoccupied":
                    freed.append((bay_id, watched_bays.pop(bay_id)))
            for bay_id, info in freed:
                street = info.get("road_description") or info.get("street") or f"Bay {bay_id}"
                ok = await send_pushover(
                    "Parking spot free!",
                    f"Bay {bay_id} on {street} is now available.",
                )
                if ok:
                    await db.delete_watched_bay(bay_id)
                else:
                    # Re-add to watch list if notification failed (#6)
                    watched_bays[bay_id] = info
        except Exception:
            api_log.exception("Error in watch loop")  # log instead of silently swallow (#6)


# ── Street helpers ────────────────────────────────────────────────────────────

def extract_street(description: str) -> str:
    match = re.match(r"^(.+?)\s+between\s+", description, re.IGNORECASE)
    return match.group(1).strip() if match else description.strip()


# ── Data fetching ─────────────────────────────────────────────────────────────

async def get_street_map() -> dict[str, str]:
    global _street_map, _street_map_fetched_at

    now = asyncio.get_running_loop().time()  # (#2)
    if _street_map and (now - _street_map_fetched_at) < STREET_MAP_TTL:
        return _street_map

    async with _street_map_lock:
        now = asyncio.get_running_loop().time()
        if _street_map and (now - _street_map_fetched_at) < STREET_MAP_TTL:
            return _street_map

        async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT) as client:
            response = await client.get(BAYS_EXPORT_API, params={"select": "kerbsideid,roadsegmentdescription"})
        if response.status_code != 200:
            raise HTTPException(status_code=502, detail=f"Melbourne API error: {response.status_code}")

        records = response.json()
        _street_map = {
            str(r["kerbsideid"]): r.get("roadsegmentdescription", "")
            for r in records if r.get("kerbsideid") is not None
        }
        _street_map_fetched_at = now
        return _street_map


async def _full_sensor_fetch() -> None:
    """Fetch all sensor records via the export endpoint (1 API call)."""
    global _last_sensor_fetch

    async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT) as client:
        response = await client.get(
            SENSOR_EXPORT_API,
            params={"select": "kerbsideid,status_description,status_timestamp,location"},
        )
    if response.status_code != 200:
        raise HTTPException(status_code=502, detail=f"Melbourne API error: {response.status_code}")

    records = response.json()
    if not records:
        api_log.warning("Full sensor fetch returned empty list — skipping bay store update")
        return
    _bay_store.clear()
    for r in records:
        if r.get("kerbsideid") is not None:
            _bay_store[r["kerbsideid"]] = r

    _last_sensor_fetch = datetime.now(timezone.utc).isoformat()

    try:
        await db.save_bay_records(list(_bay_store.values()))
        await db.set_state("last_sensor_fetch", _last_sensor_fetch)
    except Exception:
        api_log.exception("Failed to persist bay store to DB")


async def _delta_sensor_fetch() -> None:
    """Fetch only bays whose status changed since the last fetch."""
    global _last_sensor_fetch

    # Guard against malformed timestamp in DB (#4)
    raw = _last_sensor_fetch or ""
    if len(raw) < 19:
        api_log.warning("last_sensor_fetch '%s' is malformed — falling back to full fetch", raw)
        await _full_sensor_fetch()
        return

    since = raw[:19] + "+00:00"
    fetch_started_at = datetime.now(timezone.utc).isoformat()

    offset = 0
    changed_records: list[dict] = []

    async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT) as client:
        while True:
            response = await client.get(
                SENSOR_API,
                params={
                    "select": "kerbsideid,status_description,status_timestamp,location",
                    "where": f'status_timestamp > "{since}"',
                    "limit": PAGE_SIZE,
                    "offset": offset,
                },
            )
            if response.status_code != 200:
                raise HTTPException(status_code=502, detail=f"Melbourne API error: {response.status_code}")

            data  = response.json()
            page  = data.get("results", [])
            total = data.get("total_count", 0)

            for r in page:
                if r.get("kerbsideid") is not None:
                    _bay_store[r["kerbsideid"]] = r
                    changed_records.append(r)

            offset += PAGE_SIZE
            if offset >= total:
                break

    _last_sensor_fetch = fetch_started_at

    # DB write failure must not prevent the response from succeeding (#8)
    try:
        if changed_records:
            await db.save_bay_records(changed_records)
        await db.set_state("last_sensor_fetch", _last_sensor_fetch)
    except Exception:
        api_log.exception("Failed to persist delta records to DB")


async def _refresh_sensors() -> None:
    """Full fetch on first call; delta fetch on all subsequent calls."""
    if not _bay_store or _last_sensor_fetch is None:
        await _full_sensor_fetch()
    else:
        await _delta_sensor_fetch()


# ── Transform ─────────────────────────────────────────────────────────────────

def transform_record(record: dict, street_map: dict) -> Optional[dict]:
    location = record.get("location")
    if not location or "lat" not in location or "lon" not in location:
        return None
    status    = record.get("status_description", "Unknown")
    bay_id    = record.get("kerbsideid")
    road_desc = street_map.get(str(bay_id), "")
    return {
        "id": bay_id,
        "lat": location["lat"],
        "lon": location["lon"],
        "status": status,
        "occupied": status == "Present",
        "updated_at": record.get("status_timestamp"),
        "road_description": road_desc,
        "street": extract_street(road_desc) if road_desc else "",
    }


async def _get_parking_data() -> dict:
    global _parking_cache, _parking_cache_at

    now = asyncio.get_running_loop().time()  # (#2)
    if _parking_cache is not None and (now - _parking_cache_at) < PARKING_CACHE_TTL:
        return _parking_cache

    # Lock prevents concurrent requests from both triggering a refresh (#3)
    async with _refresh_lock:
        # Re-check cache after acquiring lock (another request may have refreshed)
        now = asyncio.get_running_loop().time()
        if _parking_cache is not None and (now - _parking_cache_at) < PARKING_CACHE_TTL:
            return _parking_cache

        street_map = await get_street_map()
        await _refresh_sensors()

        bays = [
            t for r in _bay_store.values()
            if (t := transform_record(r, street_map)) is not None
        ]
        occupied = sum(1 for b in bays if b["occupied"])
        result = {
            "fetched_at": datetime.now(timezone.utc).isoformat(),
            "total": len(bays),
            "occupied": occupied,
            "free": len(bays) - occupied,
            "bays": bays,
        }
        _parking_cache = result
        _parking_cache_at = asyncio.get_running_loop().time()
        return result


# ── API endpoints ─────────────────────────────────────────────────────────────

@app.get("/api/parking")
async def get_parking():
    return await _get_parking_data()


@app.get("/api/streets")
async def get_streets():
    street_map = await get_street_map()
    streets = sorted({extract_street(v) for v in street_map.values() if v})
    return {"streets": streets}


class WatchRequest(BaseModel):
    bay_id: int
    street: str
    road_description: str


MAX_WATCHED = 10


@app.post("/api/watch")
async def watch_bay(req: WatchRequest):
    if not pushover_configured():
        raise HTTPException(status_code=503, detail="Pushover is not configured on the server.")
    if req.bay_id not in watched_bays and len(watched_bays) >= MAX_WATCHED:
        raise HTTPException(status_code=400, detail=f"Watch limit reached ({MAX_WATCHED} bays maximum).")
    watched_bays[req.bay_id] = {"street": req.street, "road_description": req.road_description}
    try:
        await db.save_watched_bay(req.bay_id, req.street, req.road_description)
    except Exception:
        api_log.exception("Failed to persist watched bay %d to DB", req.bay_id)
    return {"watching": req.bay_id, "total_watched": len(watched_bays)}


@app.delete("/api/watch/{bay_id}")
async def unwatch_bay(bay_id: int):
    watched_bays.pop(bay_id, None)
    try:
        await db.delete_watched_bay(bay_id)
    except Exception:
        api_log.exception("Failed to delete watched bay %d from DB", bay_id)
    return {"unwatched": bay_id, "total_watched": len(watched_bays)}


@app.get("/api/watch")
async def get_watched():
    return {"watched": list(watched_bays.keys())}


@app.get("/api/pushover/status")
async def pushover_status():
    return {"configured": pushover_configured(), "max_watched": MAX_WATCHED}


@app.get("/health")
async def health():
    return {"status": "ok", "bays_loaded": len(_bay_store), "watched": len(watched_bays)}


app.mount("/", StaticFiles(directory="static", html=True), name="static")
