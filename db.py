"""
SQLite persistence layer for the parking app.

Tables:
  bay_store   — full sensor snapshot (one row per bay)
  app_state   — key/value store for metadata (last_sensor_fetch, etc.)
  watched_bays — bays the user is watching for Pushover notifications
"""

import json
import os

import aiosqlite

_data_dir = os.getenv("DATA_DIR", ".")
DB_PATH = os.path.join(_data_dir, "parking.db")


async def init_db() -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.executescript("""
            CREATE TABLE IF NOT EXISTS bay_store (
                kerbsideid    INTEGER PRIMARY KEY,
                record_json   TEXT    NOT NULL,
                updated_at    TEXT    NOT NULL
            );

            CREATE TABLE IF NOT EXISTS app_state (
                key   TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS watched_bays (
                bay_id           INTEGER PRIMARY KEY,
                street           TEXT NOT NULL,
                road_description TEXT NOT NULL
            );
        """)
        await db.commit()


# ── Bay store ─────────────────────────────────────────────────────────────────

async def load_bay_store() -> dict[int, dict]:
    """Return {kerbsideid: record_dict} from the DB."""
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT kerbsideid, record_json FROM bay_store") as cur:
            rows = await cur.fetchall()
    return {row[0]: json.loads(row[1]) for row in rows}


async def save_bay_records(records: list[dict]) -> None:
    """Upsert a list of raw sensor records into bay_store."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.executemany(
            """
            INSERT INTO bay_store (kerbsideid, record_json, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(kerbsideid) DO UPDATE SET
                record_json = excluded.record_json,
                updated_at  = excluded.updated_at
            """,
            [
                (
                    r["kerbsideid"],
                    json.dumps(r),
                    r.get("status_timestamp", ""),
                )
                for r in records
                if r.get("kerbsideid") is not None
            ],
        )
        await db.commit()


# ── App state ─────────────────────────────────────────────────────────────────

async def get_state(key: str) -> str | None:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT value FROM app_state WHERE key = ?", (key,)) as cur:
            row = await cur.fetchone()
    return row[0] if row else None


async def set_state(key: str, value: str) -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO app_state (key, value) VALUES (?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (key, value),
        )
        await db.commit()


# ── Watched bays ──────────────────────────────────────────────────────────────

async def load_watched_bays() -> dict[int, dict]:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT bay_id, street, road_description FROM watched_bays") as cur:
            rows = await cur.fetchall()
    return {row[0]: {"street": row[1], "road_description": row[2]} for row in rows}


async def save_watched_bay(bay_id: int, street: str, road_description: str) -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO watched_bays (bay_id, street, road_description) VALUES (?, ?, ?) "
            "ON CONFLICT(bay_id) DO UPDATE SET street = excluded.street, "
            "road_description = excluded.road_description",
            (bay_id, street, road_description),
        )
        await db.commit()


async def delete_watched_bay(bay_id: int) -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM watched_bays WHERE bay_id = ?", (bay_id,))
        await db.commit()


async def clear_watched_bays() -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM watched_bays")
        await db.commit()
