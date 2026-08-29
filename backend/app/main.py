import asyncio
import json
import os
import uuid
from contextlib import asynccontextmanager
from pathlib import Path

import httpx
import inngest.fast_api
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles

from .analysis import analyze_similarity_chunk, normalize, prepare_similarity_analysis
from .db import connect, init_db, row_dict
from .inngest_jobs import configured as inngest_configured
from .inngest_jobs import inngest_client, queue_sync, sync_spotify_playlists
from .spotify import API, access_token, authorization_url, exchange_code, spotify_get

load_dotenv()
ROOT = Path(__file__).resolve().parents[2]
FRONTEND = ROOT / "frontend"


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    yield


app = FastAPI(title="Playlist intelligence", lifespan=lifespan)
app.mount("/assets", StaticFiles(directory=FRONTEND), name="assets")
inngest.fast_api.serve(app, inngest_client, [sync_spotify_playlists])


def status(job_id, status, stage, message="", **counts):
    with connect() as db:
        db.execute("UPDATE sync_run SET status=?, stage=?, message=?, listed_playlists=COALESCE(?,listed_playlists), readable_playlists=COALESCE(?,readable_playlists), imported_items=COALESCE(?,imported_items), skipped_playlists=COALESCE(?,skipped_playlists), last_progress_at=CASE WHEN ?='running' THEN CURRENT_TIMESTAMP ELSE last_progress_at END, completed_at=CASE WHEN ? IN ('completed','completed_with_warnings','failed') THEN CURRENT_TIMESTAMP ELSE NULL END WHERE id=?", (status, stage, message, counts.get("listed"), counts.get("readable"), counts.get("items"), counts.get("skipped"), status, status, job_id))
        db.commit()


def advance_sync_progress(job_id: str, *, items: int = 0, readable: int = 0, skipped: int = 0) -> None:
    """Atomically record visible progress while parallel playlist imports run."""
    with connect() as db:
        db.execute(
            "UPDATE sync_run SET imported_items=imported_items + ?, readable_playlists=readable_playlists + ?, skipped_playlists=skipped_playlists + ?, last_progress_at=CURRENT_TIMESTAMP WHERE id=?",
            (items, readable, skipped, job_id),
        )


def upsert_track(db, item):
    track = item.get("track") or item.get("item")
    if not track or track.get("type") != "track" or track.get("is_local"):
        return None, (track or {}).get("type", "unknown")
    spotify_id = track.get("id")
    if not spotify_id:
        return None, "unknown"
    album = track.get("album") or {}
    date = album.get("release_date")
    try: year = int((date or "")[:4])
    except ValueError: year = None
    db.execute("INSERT INTO track(spotify_id,isrc,name,normalized_name,duration_ms,album_name,release_date,release_year,linked_from_id,is_local) VALUES(?,?,?,?,?,?,?,?,?,0) ON CONFLICT(spotify_id) DO UPDATE SET name=excluded.name, normalized_name=excluded.normalized_name, duration_ms=excluded.duration_ms, album_name=excluded.album_name, release_date=excluded.release_date, release_year=excluded.release_year", (spotify_id, (track.get("external_ids") or {}).get("isrc"), track.get("name", "Unknown track"), normalize(track.get("name")), track.get("duration_ms"), album.get("name"), date, year, (track.get("linked_from") or {}).get("id")))
    track_id = db.execute("SELECT id FROM track WHERE spotify_id=?", (spotify_id,)).fetchone()["id"]
    db.execute("DELETE FROM track_artist WHERE track_id=?", (track_id,))
    for order, artist in enumerate(track.get("artists") or []):
        db.execute("INSERT INTO artist(spotify_id,name,normalized_name) VALUES(?,?,?) ON CONFLICT(spotify_id) DO UPDATE SET name=excluded.name", (artist.get("id"), artist.get("name", "Unknown artist"), normalize(artist.get("name"))))
        artist_id = db.execute("SELECT id FROM artist WHERE spotify_id=?", (artist.get("id"),)).fetchone()["id"]
        db.execute("INSERT INTO track_artist(track_id,artist_id,ordinal) VALUES(?,?,?) ON CONFLICT DO NOTHING", (track_id, artist_id, order))
    return track_id, "track"


async def prepare_sync(job_id: str) -> dict:
    token = await access_token()
    with connect() as db:
        account = db.execute("SELECT spotify_user_id FROM oauth_token WHERE id=1").fetchone()
    owner_id = account["spotify_user_id"]
    status(job_id, "running", "listing playlists")
    async with httpx.AsyncClient(timeout=25) as client:
        playlists, url, params = [], f"{API}/me/playlists", {"limit": 50}
        while url:
            response = await spotify_get(client, url, token, params)
            response.raise_for_status(); payload = response.json(); playlists.extend(payload.get("items", [])); url = payload.get("next"); params = None
    playlists = [playlist for playlist in playlists if (playlist.get("owner") or {}).get("id") == owner_id]
    with connect() as db:
        db.execute("DELETE FROM playlist WHERE owner_id IS NOT NULL AND owner_id != ?", (owner_id,))
        for playlist in playlists:
            db.execute("INSERT INTO playlist(spotify_id,name,owner_id,snapshot_id,track_total,readable,error_reason,spotify_url) VALUES(?,?,?,?,?,1,NULL,?) ON CONFLICT(spotify_id) DO UPDATE SET name=excluded.name, owner_id=excluded.owner_id, snapshot_id=excluded.snapshot_id, track_total=excluded.track_total, readable=1, error_reason=NULL, spotify_url=excluded.spotify_url, updated_at=CURRENT_TIMESTAMP", (playlist["id"], playlist.get("name", "Untitled"), owner_id, playlist.get("snapshot_id"), (playlist.get("items") or playlist.get("tracks") or {}).get("total", 0), (playlist.get("external_urls") or {}).get("spotify")))
    status(job_id, "running", "importing items", listed=len(playlists))
    return {"owner_id": owner_id, "playlist_ids": [playlist["id"] for playlist in playlists]}


async def import_playlist_page(job_id: str, spotify_id: str, offset: int) -> dict:
    """Import ten songs per durable step to stay below Vercel's timeout."""
    token = await access_token()

    with connect() as db:
        row = db.execute("SELECT id FROM playlist WHERE spotify_id=?", (spotify_id,)).fetchone()
        if not row:
            raise ValueError("Playlist is missing from the current sync")
        if offset == 0:
            db.execute("DELETE FROM playlist_item WHERE playlist_id=?", (row["id"],))
    try:
        async with httpx.AsyncClient(timeout=25) as client:
            response = await spotify_get(client, f"{API}/playlists/{spotify_id}/items", token, {"limit": 10, "offset": offset})
            if response.status_code == 403:
                with connect() as db: db.execute("UPDATE playlist SET readable=0,error_reason=? WHERE id=?", ("Spotify denied access to this playlist's items", row["id"]))
                advance_sync_progress(job_id, skipped=1)
                return {"done": True, "readable": 0, "skipped": 1, "items": 0}
            response.raise_for_status(); payload = response.json()
    except httpx.HTTPError as error:
        with connect() as db: db.execute("UPDATE playlist SET readable=0,error_reason=? WHERE id=?", (str(error)[:200], row["id"]))
        advance_sync_progress(job_id, skipped=1)
        return {"done": True, "readable": 0, "skipped": 1, "items": 0}

    inserted = 0
    with connect() as db:
        for position, item in enumerate(payload.get("items", []), start=offset):
            track_id, item_type = upsert_track(db, item)
            result = db.execute("INSERT INTO playlist_item(playlist_id,position,track_id,item_type,added_at) VALUES(?,?,?,?,?) ON CONFLICT DO NOTHING RETURNING position", (row["id"], position, track_id, item_type, item.get("added_at"))).fetchone()
            inserted += int(result is not None)
    # Page-level progress stays visible even when a playlist contains thousands
    # of songs. Duplicate step retries do not inflate the counter.
    advance_sync_progress(job_id, items=inserted)
    next_offset = offset + len(payload.get("items", []))
    done = not payload.get("next")
    if done:
        advance_sync_progress(job_id, readable=1)
    return {"done": done, "next_offset": next_offset, "readable": int(done), "skipped": 0, "items": inserted}


def _sync_counts(results: list[dict]) -> dict[str, int]:
    readable = sum(result["readable"] for result in results)
    skipped = sum(result["skipped"] for result in results)
    items = sum(result["items"] for result in results)
    return {"readable": readable, "skipped": skipped, "items": items}


async def prepare_similarity(job_id: str, owner_id: str, results: list[dict]) -> dict:
    counts = _sync_counts(results)
    status(job_id, "running", "preparing similarities", "Preparing playlist comparisons", **counts)
    with connect() as db:
        playlist_ids = prepare_similarity_analysis(db, owner_id)
    return {"playlist_ids": playlist_ids, **counts}


async def analyze_similarity_step(job_id: str, playlist_ids: list[int], start: int, end: int, counts: dict) -> dict:
    with connect() as db:
        stored_pairs = analyze_similarity_chunk(db, playlist_ids, start, end)
    status(
        job_id,
        "running",
        "analyzing similarities",
        f"Compared {end} of {len(playlist_ids)} playlists",
        **counts,
    )
    return {"stored_pairs": stored_pairs}


async def finalize_sync(job_id: str, counts: dict) -> dict:
    skipped = counts["skipped"]
    final = "completed_with_warnings" if skipped else "completed"
    status(job_id, final, "complete", "Analysis is ready", **counts)
    return {"status": final}


async def run_sync(job_id: str, raise_on_error: bool = False):
    try:
        prepared = await prepare_sync(job_id)
        results = []
        for spotify_id in prepared["playlist_ids"]:
            offset = imported = 0
            while True:
                page = await import_playlist_page(job_id, spotify_id, offset)
                imported += page["items"]
                if page["done"]:
                    results.append({"readable": page["readable"], "skipped": page["skipped"], "items": imported})
                    break
                offset = page["next_offset"]
        analysis = await prepare_similarity(job_id, prepared["owner_id"], results)
        playlist_ids = analysis["playlist_ids"]
        for start in range(0, max(len(playlist_ids) - 1, 0), 4):
            await analyze_similarity_step(job_id, playlist_ids, start, min(start + 4, len(playlist_ids) - 1), analysis)
        await finalize_sync(job_id, analysis)
    except Exception as error:
        status(job_id, "failed", "failed", str(error)[:300])
        if raise_on_error:
            raise


@app.get("/")
def home(): return FileResponse(FRONTEND / "index.html")

@app.get("/api/health")
def health():
    with connect() as db: connected = bool(db.execute("SELECT 1 FROM oauth_token WHERE id=1").fetchone())
    return {"ok": True, "spotify_connected": connected}

@app.get("/api/auth/spotify/start")
def start_auth():
    try: return RedirectResponse(authorization_url())
    except RuntimeError as error: raise HTTPException(400, str(error))

@app.get("/api/auth/spotify/callback")
async def auth_callback(code: str | None = None, state: str | None = None, error: str | None = None):
    if error: return RedirectResponse(f"/?auth_error={error}")
    try: await exchange_code(code or "", state or "")
    except Exception as exc: return RedirectResponse(f"/?auth_error={str(exc)[:100]}")
    return RedirectResponse("/?connected=1")

@app.post("/api/auth/disconnect")
def disconnect():
    with connect() as db: db.execute("DELETE FROM oauth_token"); db.commit()
    return {"ok": True}

@app.post("/api/sync")
async def sync():
    with connect() as db:
        if not db.execute("SELECT 1 FROM oauth_token WHERE id=1").fetchone(): raise HTTPException(400, "Connect Spotify first")
        # A job that stops reporting progress has lost its worker. Each Spotify
        # page refreshes this heartbeat, so real imports are never replaced.
        db.execute("UPDATE sync_run SET status='failed', stage='failed', message='Sync stopped reporting progress. Please retry.', completed_at=CURRENT_TIMESTAMP WHERE status='running' AND COALESCE(last_progress_at,started_at) < CURRENT_TIMESTAMP - INTERVAL '10 minutes'")
        existing = db.execute("SELECT id FROM sync_run WHERE status='running'").fetchone()
        if existing: return {"job_id": existing["id"]}
        job_id = str(uuid.uuid4()); db.execute("INSERT INTO sync_run(id,status,stage,message) VALUES(?,?,?,?)", (job_id, "running", "queued", "Sync queued")); db.commit()
    if inngest_configured():
        try:
            await queue_sync(job_id)
        except Exception:
            status(job_id, "failed", "failed", "Could not queue the Spotify sync")
            raise HTTPException(503, "Could not queue the Spotify sync")
        return {"job_id": job_id, "runner": "inngest"}

    if os.getenv("VERCEL"):
        status(job_id, "failed", "failed", "Inngest is not configured")
        raise HTTPException(503, "Sync is not configured yet")

    # Local development can still run without an Inngest account.
    asyncio.create_task(run_sync(job_id))
    return {"job_id": job_id, "runner": "local"}

@app.get("/api/sync/{job_id}")
def sync_status(job_id: str):
    with connect() as db: row = db.execute("SELECT * FROM sync_run WHERE id=?", (job_id,)).fetchone()
    if not row: raise HTTPException(404, "Sync job not found")
    return row_dict(row)

@app.get("/api/dashboard")
def dashboard():
    with connect() as db:
        account = db.execute("SELECT spotify_user_id FROM oauth_token WHERE id=1").fetchone()
        owner_id = account["spotify_user_id"] if account else ""
        totals = db.execute("SELECT COUNT(*) playlists, COALESCE(SUM(readable),0) readable, COALESCE(SUM(track_total),0) items FROM playlist WHERE owner_id=?", (owner_id,)).fetchone()
        pairs = db.execute("SELECT p1.name a,p2.name b,pp.* FROM playlist_pair pp JOIN playlist p1 ON p1.id=pp.playlist_a_id JOIN playlist p2 ON p2.id=pp.playlist_b_id WHERE pp.jaccard > 0 ORDER BY pp.jaccard DESC LIMIT 8").fetchall()
        playlist_rows = db.execute("SELECT p.id,p.name,a.genre_json FROM assessment a JOIN playlist p ON p.id=a.playlist_id WHERE p.owner_id=? ORDER BY LOWER(p.name)", (owner_id,)).fetchall()
    playlists = [row_dict(x) for x in playlist_rows]
    for playlist in playlists:
        playlist["genres"] = json.loads(playlist.pop("genre_json") or "[]")
    return {"totals": row_dict(totals), "pairs": [row_dict(x) for x in pairs], "playlists": playlists}

@app.get("/api/playlists")
def playlists():
    with connect() as db:
        account = db.execute("SELECT spotify_user_id FROM oauth_token WHERE id=1").fetchone()
        rows = db.execute("SELECT p.id,p.name,p.track_total,p.readable,p.error_reason,p.spotify_url,a.summary,a.genre_json FROM playlist p LEFT JOIN assessment a ON a.playlist_id=p.id WHERE p.owner_id=? ORDER BY LOWER(p.name)", (account["spotify_user_id"] if account else "",)).fetchall()
    playlists = [row_dict(row) for row in rows]
    for playlist in playlists:
        playlist["genres"] = json.loads(playlist.pop("genre_json") or "[]")
    return playlists

@app.get("/api/playlists/{playlist_id}")
def playlist_detail(playlist_id: int):
    with connect() as db:
        account = db.execute("SELECT spotify_user_id FROM oauth_token WHERE id=1").fetchone()
        owner_id = account["spotify_user_id"] if account else ""
        playlist = db.execute("SELECT p.* FROM playlist p WHERE p.id=? AND p.owner_id=?", (playlist_id, owner_id)).fetchone()
        if not playlist: raise HTTPException(404, "Playlist not found")
        comparisons = db.execute("SELECT p.id,p.name,pp.* FROM playlist_pair pp JOIN playlist p ON p.id=CASE WHEN pp.playlist_a_id=? THEN pp.playlist_b_id ELSE pp.playlist_a_id END WHERE (pp.playlist_a_id=? OR pp.playlist_b_id=?) AND pp.jaccard > 0 ORDER BY pp.jaccard DESC", (playlist_id, playlist_id, playlist_id)).fetchall()
        tracks = db.execute("""SELECT pi.position,t.name
            FROM playlist_item pi LEFT JOIN track t ON t.id=pi.track_id
            WHERE pi.playlist_id=? ORDER BY pi.position""", (playlist_id,)).fetchall()
    data = row_dict(playlist)
    data["comparisons"] = [row_dict(row) for row in comparisons]
    data["tracks"] = [row_dict(row) for row in tracks]
    return data

@app.get("/api/comparisons/{playlist_a_id}/{playlist_b_id}")
def comparison_detail(playlist_a_id: int, playlist_b_id: int):
    with connect() as db:
        account = db.execute("SELECT spotify_user_id FROM oauth_token WHERE id=1").fetchone()
        owner_id = account["spotify_user_id"] if account else ""
        pair = db.execute("SELECT pp.*,p1.name a_name,p2.name b_name FROM playlist_pair pp JOIN playlist p1 ON p1.id=pp.playlist_a_id JOIN playlist p2 ON p2.id=pp.playlist_b_id WHERE ((pp.playlist_a_id=? AND pp.playlist_b_id=?) OR (pp.playlist_a_id=? AND pp.playlist_b_id=?)) AND p1.owner_id=? AND p2.owner_id=?", (playlist_a_id, playlist_b_id, playlist_b_id, playlist_a_id, owner_id, owner_id)).fetchone()
        if not pair or pair["jaccard"] == 0: raise HTTPException(404, "These playlists do not share tracks")
        tracks = db.execute("SELECT t.name,a.name artist,STRING_AGG(DISTINCT CASE WHEN pi.playlist_id=? THEN pi.position::TEXT END, ',') a_positions,STRING_AGG(DISTINCT CASE WHEN pi.playlist_id=? THEN pi.position::TEXT END, ',') b_positions FROM playlist_item pi JOIN track t ON t.id=pi.track_id LEFT JOIN track_artist ta ON ta.track_id=t.id AND ta.ordinal=0 LEFT JOIN artist a ON a.id=ta.artist_id WHERE pi.playlist_id IN (?,?) GROUP BY t.id,a.name HAVING COUNT(DISTINCT pi.playlist_id)=2 ORDER BY t.name", (playlist_a_id, playlist_b_id, playlist_a_id, playlist_b_id)).fetchall()
    return {"pair": row_dict(pair), "tracks": [row_dict(row) for row in tracks]}

@app.delete("/api/local-data")
def delete_data():
    with connect() as db:
        for table in ("playlist_pair", "assessment", "playlist_item", "track_artist", "artist", "track", "playlist", "sync_run", "oauth_token", "oauth_pending"): db.execute(f"DELETE FROM {table}")
        db.commit()
    return {"ok": True}
