import asyncio
import logging
import logging.handlers
import os
import re
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Optional

import httpx
from cachetools import TTLCache
from dotenv import load_dotenv

import db
from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

load_dotenv()

# ── Logging ───────────────────────────────────────────────────────────────────

_data_dir = os.getenv("DATA_DIR", ".")
os.makedirs(_data_dir, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.handlers.RotatingFileHandler(
            os.path.join(_data_dir, "api_calls.log"),
            maxBytes=5 * 1024 * 1024,  # 5 MB
            backupCount=3,
            encoding="utf-8",
        ),
        logging.StreamHandler(),
    ],
)
api_log = logging.getLogger("melbourne_api")


def _log_call(method: str, url: str, status: int, elapsed_ms: float, records: int | None = None) -> None:
    extra = f" — {records} records" if records is not None else ""
    api_log.info("%s %s → %d (%.0f ms)%s", method, url, status, elapsed_ms, extra)


@asynccontextmanager
async def lifespan(_: FastAPI):
    await db.init_db()

    # Restore bay store from DB (avoids full API fetch on restart)
    stored = await db.load_bay_store()
    if stored:
        global _last_sensor_fetch
        _bay_store.update(stored)
        _last_sensor_fetch = await db.get_state("last_sensor_fetch")
        api_log.info("Restored %d bays from DB (last fetch: %s)", len(stored), _last_sensor_fetch)

    # Restore watched bays from DB
    watched = await db.load_watched_bays()
    watched_bays.update(watched)
    if watched:
        api_log.info("Restored %d watched bays from DB", len(watched))

    asyncio.create_task(_watch_loop())
    yield


app = FastAPI(title="Melbourne Parking Map", lifespan=lifespan)

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

# Static bay/street mapping: cache 24 hours
street_cache: TTLCache = TTLCache(maxsize=1, ttl=86400)
REQUEST_TIMEOUT = httpx.Timeout(connect=10, read=60, write=10, pool=5)

# {bay_id: {"street": str, "road_description": str}} — bays being watched
watched_bays: dict[int, dict] = {}

# ── Sensor state (delta fetch) ────────────────────────────────────────────────
# Full bay data store: {kerbsideid: raw_record_dict}
_bay_store: dict[int, dict] = {}
# ISO timestamp of last successful sensor fetch (used as delta filter)
_last_sensor_fetch: str | None = None


# ── Pushover ──────────────────────────────────────────────────────────────────

def pushover_configured() -> bool:
    return bool(os.getenv("PUSHOVER_API_TOKEN")) and bool(os.getenv("PUSHOVER_USER_KEY"))


async def send_pushover(title: str, message: str) -> None:
    async with httpx.AsyncClient(timeout=10) as client:
        await client.post(PUSHOVER_API, data={
            "token": os.getenv("PUSHOVER_API_TOKEN"),
            "user":  os.getenv("PUSHOVER_USER_KEY"),
            "title": title,
            "message": message,
        })


# ── Background watcher ────────────────────────────────────────────────────────

async def _watch_loop() -> None:
    """Every 2 minutes, check if any watched bay has become free and notify.
    Reuses the in-memory bay store rather than making extra API calls."""
    while True:
        await asyncio.sleep(120)
        if not watched_bays or not _bay_store:
            continue
        try:
            freed = []
            for bay_id in list(watched_bays.keys()):
                record = _bay_store.get(bay_id)
                if record and record.get("status_description") != "Present":
                    freed.append((bay_id, watched_bays.pop(bay_id)))
            for bay_id, info in freed:
                street = info.get("road_description") or info.get("street") or f"Bay {bay_id}"
                await send_pushover(
                    "Parking spot free!",
                    f"Bay {bay_id} on {street} is now available.",
                )
        except Exception:
            pass


# ── Street helpers ────────────────────────────────────────────────────────────

def extract_street(description: str) -> str:
    match = re.match(r"^(.+?)\s+between\s+", description, re.IGNORECASE)
    return match.group(1).strip() if match else description.strip()


# ── Data fetching ─────────────────────────────────────────────────────────────

async def get_street_map() -> dict[str, str]:
    if "map" in street_cache:
        api_log.info("GET %s → cache hit", BAYS_EXPORT_API)
        return street_cache["map"]

    t0 = asyncio.get_event_loop().time()
    async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT) as client:
        response = await client.get(BAYS_EXPORT_API, params={"select": "kerbsideid,roadsegmentdescription"})
    elapsed = (asyncio.get_event_loop().time() - t0) * 1000
    _log_call("GET", BAYS_EXPORT_API, response.status_code, elapsed)
    if response.status_code != 200:
        raise HTTPException(status_code=502, detail=f"Melbourne API error: {response.status_code}")

    records = response.json()
    mapping = {
        str(r["kerbsideid"]): r.get("roadsegmentdescription", "")
        for r in records if r.get("kerbsideid") is not None
    }
    street_cache["map"] = mapping
    return mapping


async def _full_sensor_fetch() -> None:
    """Fetch all sensor records via the export endpoint (1 API call).
    Populates _bay_store and sets _last_sensor_fetch."""
    global _last_sensor_fetch

    t0 = asyncio.get_event_loop().time()
    async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT) as client:
        response = await client.get(
            SENSOR_EXPORT_API,
            params={"select": "kerbsideid,status_description,status_timestamp,location"},
        )
    elapsed = (asyncio.get_event_loop().time() - t0) * 1000
    _log_call("GET", SENSOR_EXPORT_API, response.status_code, elapsed, None)

    if response.status_code != 200:
        raise HTTPException(status_code=502, detail=f"Melbourne API error: {response.status_code}")

    records = response.json()
    _bay_store.clear()
    for r in records:
        if r.get("kerbsideid") is not None:
            _bay_store[r["kerbsideid"]] = r

    _last_sensor_fetch = datetime.now(timezone.utc).isoformat()
    await db.save_bay_records(list(_bay_store.values()))
    await db.set_state("last_sensor_fetch", _last_sensor_fetch)
    api_log.info("Full sensor fetch complete: %d bays stored", len(_bay_store))


async def _delta_sensor_fetch() -> None:
    """Fetch only bays whose status changed since the last fetch (1–few API calls).
    Merges updates into _bay_store and advances _last_sensor_fetch."""
    global _last_sensor_fetch

    # Strip sub-second precision — ODS timestamp filter doesn't need it
    since = _last_sensor_fetch[:19] + "+00:00"
    fetch_started_at = datetime.now(timezone.utc).isoformat()

    offset = 0
    total_updated = 0
    changed_records: list[dict] = []

    async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT) as client:
        while True:
            t0 = asyncio.get_event_loop().time()
            response = await client.get(
                SENSOR_API,
                params={
                    "select": "kerbsideid,status_description,status_timestamp,location",
                    "where": f'status_timestamp > "{since}"',
                    "limit": PAGE_SIZE,
                    "offset": offset,
                },
            )
            elapsed = (asyncio.get_event_loop().time() - t0) * 1000

            if response.status_code != 200:
                _log_call("GET", SENSOR_API, response.status_code, elapsed)
                raise HTTPException(status_code=502, detail=f"Melbourne API error: {response.status_code}")

            data    = response.json()
            page    = data.get("results", [])
            total   = data.get("total_count", 0)
            _log_call("GET", SENSOR_API, response.status_code, elapsed, len(page))

            for r in page:
                if r.get("kerbsideid") is not None:
                    _bay_store[r["kerbsideid"]] = r
                    changed_records.append(r)
            total_updated += len(page)

            offset += PAGE_SIZE
            if offset >= total:
                break

    _last_sensor_fetch = fetch_started_at
    if changed_records:
        await db.save_bay_records(changed_records)
    await db.set_state("last_sensor_fetch", _last_sensor_fetch)
    api_log.info("Delta sensor fetch complete: %d bays updated (total store: %d)", total_updated, len(_bay_store))


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


# Cache of the fully assembled /api/parking response (2 min TTL)
_parking_cache: dict | None = None
_parking_cache_at: float = 0.0
PARKING_CACHE_TTL = 120  # seconds


async def _get_parking_data() -> dict:
    global _parking_cache, _parking_cache_at

    now = asyncio.get_event_loop().time()
    if _parking_cache is not None and (now - _parking_cache_at) < PARKING_CACHE_TTL:
        api_log.info("GET %s → cache hit", SENSOR_API)
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
    _parking_cache_at = now
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
    await db.save_watched_bay(req.bay_id, req.street, req.road_description)
    return {"watching": req.bay_id, "total_watched": len(watched_bays)}


@app.delete("/api/watch/{bay_id}")
async def unwatch_bay(bay_id: int):
    watched_bays.pop(bay_id, None)
    await db.delete_watched_bay(bay_id)
    return {"unwatched": bay_id, "total_watched": len(watched_bays)}


@app.get("/api/watch")
async def get_watched():
    return {"watched": list(watched_bays.keys())}


@app.get("/api/pushover/status")
async def pushover_status():
    return {"configured": pushover_configured()}


app.mount("/", StaticFiles(directory="static", html=True), name="static")
