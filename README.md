# Playlist intelligence

Local, read-only Spotify playlist analysis. It finds playlist overlap and a genre breakdown based on primary-artist tags. It does not change Spotify playlists.

## Run it

1. Create a Spotify app and add `http://127.0.0.1:8000/api/auth/spotify/callback` as a redirect URI.
2. Copy `.env.example` to `.env`, then set `SPOTIFY_CLIENT_ID` and `DATABASE_URL`. Use the Supabase pooled Postgres connection string and keep its password out of Git.
3. Create a virtual environment, install `backend/requirements.txt`, then run:

```powershell
uvicorn app.main:app --app-dir backend --reload
```

4. Open `http://127.0.0.1:8000`.

The app requests only playlist read scopes. Spotify access and data-retention rules change, so read `docs/spotify-notes.md` before using it beyond personal testing.
