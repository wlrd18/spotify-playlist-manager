import json
import math
import re
import sqlite3
from collections import Counter


def normalize(value: str | None) -> str:
    value = (value or "").lower()
    value = re.sub(r"\s*\([^)]*(remaster(?:ed)?|radio edit|live|deluxe)[^)]*\)", "", value)
    return re.sub(r"[^a-z0-9]+", " ", value).strip()


def jaccard(a: set[int], b: set[int]) -> float:
    return len(a & b) / len(a | b) if a | b else 0.0


def cosine(left: Counter, right: Counter) -> float:
    keys = set(left) | set(right)
    numerator = sum(left[k] * right[k] for k in keys)
    denominator = math.sqrt(sum(v * v for v in left.values())) * math.sqrt(sum(v * v for v in right.values()))
    return numerator / denominator if denominator else 0.0


def entropy_concentration(values: Counter) -> float:
    total = sum(values.values())
    if total < 2 or len(values) < 2:
        return 1.0
    entropy = -sum((count / total) * math.log(count / total) for count in values.values())
    return max(0.0, 1 - entropy / math.log(len(values)))


def genre_breakdown(rows: list[sqlite3.Row], track_total: int, limit: int = 5) -> list[dict]:
    counts = Counter({row["genre"]: row["tracks"] for row in rows})
    if not track_total:
        return []
    return [{"name": genre, "tracks": tracks, "percent": round(tracks / track_total * 100)} for genre, tracks in counts.most_common(limit)]


def detect_outliers(items: list[dict]) -> None:
    usable = [item for item in items if item.get("artist_id") or item.get("genres")]
    if len(usable) < 5:
        return
    artist_counts = Counter(item["artist_id"] for item in usable if item.get("artist_id"))
    genre_counts = Counter(genre for item in usable for genre in set(item.get("genres", [])))
    core_strength = max([count / len(usable) for count in artist_counts.values()] + [count / len(usable) for count in genre_counts.values()] or [0])
    if core_strength < .40:
        return
    for item in usable:
        artist_support = (artist_counts[item["artist_id"]] - 1) / (len(usable) - 1) if item.get("artist_id") else 0
        genre_support = max(((genre_counts[genre] - 1) / (len(usable) - 1) for genre in item.get("genres", [])), default=0)
        support = max(artist_support, genre_support)
        if support < .15:
            item["outlier"] = True
            item["outlier_score"] = round((1 - support) * 100)


def analyze(db: sqlite3.Connection, owner_id: str, minimum_tracks: int = 8) -> None:
    db.execute("DELETE FROM assessment")
    db.execute("DELETE FROM playlist_pair")
    playlists = db.execute("SELECT * FROM playlist WHERE readable = 1 AND owner_id = ? ORDER BY id", (owner_id,)).fetchall()
    profiles = {}
    for playlist in playlists:
        rows = db.execute("""SELECT pi.position, t.id AS track_id, t.spotify_id
            FROM playlist_item pi LEFT JOIN track t ON t.id=pi.track_id
            WHERE pi.playlist_id=?""", (playlist["id"],)).fetchall()
        known = [r for r in rows if r["track_id"] is not None]
        track_ids = {r["track_id"] for r in known}
        # Genre analysis is intentionally paused. Similarity currently uses exact shared
        # track IDs only, so a fresh sync stays fast and needs no artist metadata calls.
        genres = []
        score = None
        confidence = 0.0
        summary = "Playlist similarity is based on exact shared tracks."
        factors = {"known_tracks": len(known), "label": "exact track overlap"}
        db.execute("INSERT INTO assessment(playlist_id,score,confidence,summary,factors_json,genre_json) VALUES(?,?,?,?,?,?) ON CONFLICT(playlist_id) DO UPDATE SET score=EXCLUDED.score,confidence=EXCLUDED.confidence,summary=EXCLUDED.summary,factors_json=EXCLUDED.factors_json,genre_json=EXCLUDED.genre_json", (playlist["id"], score, confidence, summary, json.dumps(factors), json.dumps(genres)))
        profiles[playlist["id"]] = {"tracks": track_ids, "artists": Counter(), "years": Counter()}
    for index, left in enumerate(playlists):
        for right in playlists[index + 1:]:
            a, b = profiles[left["id"]], profiles[right["id"]]
            shared = len(a["tracks"] & b["tracks"])
            overlap = jaccard(a["tracks"], b["tracks"])
            containment = shared / min(len(a["tracks"]), len(b["tracks"])) if a["tracks"] and b["tracks"] else 0.0
            artist_similarity, year_similarity = cosine(a["artists"], b["artists"]), cosine(a["years"], b["years"])
            if overlap >= .80 or containment >= .90:
                label = "near-content duplicate"
            elif overlap >= .55:
                label = "strong overlap"
            elif overlap >= .25 or max(artist_similarity, year_similarity) >= .75:
                label = "related collection"
            else:
                label = "distinct"
            db.execute("INSERT INTO playlist_pair VALUES(?,?,?,?,?,?,?,?)", (left["id"], right["id"], shared, overlap, containment, artist_similarity, year_similarity, label))
    db.commit()
