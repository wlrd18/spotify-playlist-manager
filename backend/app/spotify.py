import base64
import hashlib
import os
import secrets
import time
from urllib.parse import urlencode

import httpx

from .db import connect

API = "https://api.spotify.com/v1"
ACCOUNTS = "https://accounts.spotify.com/authorize"
TOKEN_URL = "https://accounts.spotify.com/api/token"
SCOPES = "playlist-read-private playlist-read-collaborative"


def callback_url() -> str:
    return os.getenv("SPOTIFY_REDIRECT_URI", "http://127.0.0.1:8000/api/auth/spotify/callback")


def authorization_url() -> str:
    client_id = os.getenv("SPOTIFY_CLIENT_ID")
    if not client_id:
        raise RuntimeError("SPOTIFY_CLIENT_ID is not set")
    verifier = secrets.token_urlsafe(64)
    challenge = base64.urlsafe_b64encode(hashlib.sha256(verifier.encode()).digest()).rstrip(b"=").decode()
    state = secrets.token_urlsafe(24)
    with connect() as db:
        db.execute("INSERT INTO oauth_pending(state,verifier) VALUES(?,?)", (state, verifier))
        db.commit()
    return ACCOUNTS + "?" + urlencode({"client_id": client_id, "response_type": "code", "redirect_uri": callback_url(), "scope": SCOPES, "state": state, "code_challenge_method": "S256", "code_challenge": challenge})


async def exchange_code(code: str, state: str) -> None:
    with connect() as db:
        pending = db.execute("SELECT verifier FROM oauth_pending WHERE state=?", (state,)).fetchone()
        db.execute("DELETE FROM oauth_pending WHERE state=?", (state,))
        db.commit()
    if not pending:
        raise RuntimeError("The login request expired or has an invalid state")
    payload = {"client_id": os.environ["SPOTIFY_CLIENT_ID"], "grant_type": "authorization_code", "code": code, "redirect_uri": callback_url(), "code_verifier": pending["verifier"]}
    async with httpx.AsyncClient(timeout=20) as client:
        response = await client.post(TOKEN_URL, data=payload)
        response.raise_for_status()
        token = response.json()
        profile = await client.get(f"{API}/me", headers={"Authorization": f"Bearer {token['access_token']}"})
        profile.raise_for_status()
    with connect() as db:
        db.execute("INSERT INTO oauth_token(id,access_token,refresh_token,expires_at,spotify_user_id) VALUES(1,?,?,?,?) ON CONFLICT(id) DO UPDATE SET access_token=EXCLUDED.access_token,refresh_token=EXCLUDED.refresh_token,expires_at=EXCLUDED.expires_at,spotify_user_id=EXCLUDED.spotify_user_id", (token["access_token"], token.get("refresh_token"), int(time.time()) + token.get("expires_in", 3600), profile.json().get("id")))
        db.commit()


async def access_token() -> str:
    with connect() as db:
        record = db.execute("SELECT * FROM oauth_token WHERE id=1").fetchone()
    if not record:
        raise RuntimeError("Spotify is not connected")
    if record["expires_at"] > time.time() + 60:
        return record["access_token"]
    if not record["refresh_token"]:
        raise RuntimeError("Spotify login expired. Connect again.")
    async with httpx.AsyncClient(timeout=20) as client:
        response = await client.post(TOKEN_URL, data={"client_id": os.environ["SPOTIFY_CLIENT_ID"], "grant_type": "refresh_token", "refresh_token": record["refresh_token"]})
        response.raise_for_status()
        token = response.json()
    with connect() as db:
        db.execute("UPDATE oauth_token SET access_token=?, refresh_token=?, expires_at=? WHERE id=1", (token["access_token"], token.get("refresh_token", record["refresh_token"]), int(time.time()) + token.get("expires_in", 3600)))
        db.commit()
    return token["access_token"]


async def spotify_get(client: httpx.AsyncClient, url: str, token: str, params=None):
    response = await client.get(url, params=params, headers={"Authorization": f"Bearer {token}"})
    if response.status_code == 429:
        await __import__("asyncio").sleep(int(response.headers.get("Retry-After", "1")))
        response = await client.get(url, params=params, headers={"Authorization": f"Bearer {token}"})
    return response
