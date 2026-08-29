import json
import math
import re
from collections import Counter
from typing import Any, Mapping


def normalize(value: str | None) -> str:
    value = (value or "").lower()
    value = re.sub(r"\s*\([^)]*(remaster(?:ed)?|radio edit|live|deluxe)[^)]*\)", "", value)
    return re.sub(r"[^a-z0-9]+", " ", value).strip()


def jaccard(a: set[int], b: set[int]) -> float:
    return len(a & b) / len(a | b) if a | b else 0.0


def entropy_concentration(values: Counter) -> float:
    total = sum(values.values())
    if total < 2 or len(values) < 2:
        return 1.0
    entropy = -sum((count / total) * math.log(count / total) for count in values.values())
    return max(0.0, 1 - entropy / math.log(len(values)))


def genre_breakdown(rows: list[Mapping[str, Any]], track_total: int, limit: int = 5) -> list[dict]:
    # Retained for the planned artist-genre feature; sync does not call Spotify
    # artist enrichment today because it would make imports substantially slower.
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


def prepare_similarity_analysis(db, owner_id: str) -> list[int]:
    """Reset this owner's derived data and create the lightweight playlist profiles."""
    playlist_rows = db.execute(
        "SELECT id FROM playlist WHERE readable=1 AND owner_id=? ORDER BY id", (owner_id,)
    ).fetchall()
    playlist_ids = [row["id"] for row in playlist_rows]
    if not playlist_ids:
        return []

    # This is a single-user app today, but scope deletion to the connected owner so
    # a future multi-user version cannot erase another account's analysis.
    db.execute(
        "DELETE FROM playlist_pair WHERE playlist_a_id IN (SELECT id FROM playlist WHERE owner_id=?) OR playlist_b_id IN (SELECT id FROM playlist WHERE owner_id=?)",
        (owner_id, owner_id),
    )
    db.execute(
        "DELETE FROM assessment WHERE playlist_id IN (SELECT id FROM playlist WHERE owner_id=?)",
        (owner_id,),
    )
    track_counts = {
        row["playlist_id"]: row["known_tracks"]
        for row in db.execute(
            "SELECT playlist_id, COUNT(track_id) AS known_tracks FROM playlist_item WHERE playlist_id = ANY(?) GROUP BY playlist_id",
            (playlist_ids,),
        ).fetchall()
    }
    assessment_rows = [
        (
            playlist_id,
            None,
            0.0,
            "Playlist similarity is based on exact shared tracks.",
            json.dumps({"known_tracks": track_counts.get(playlist_id, 0), "label": "exact track overlap"}),
            "[]",
        )
        for playlist_id in playlist_ids
    ]
    db.executemany(
        "INSERT INTO assessment(playlist_id,score,confidence,summary,factors_json,genre_json) VALUES(?,?,?,?,?,?)",
        assessment_rows,
    )
    return playlist_ids


def _classification(overlap: float, containment: float) -> str:
    if overlap >= 0.80 or containment >= 0.90:
        return "near-content duplicate"
    if overlap >= 0.55:
        return "strong overlap"
    return "related collection" if overlap >= 0.25 else "exact overlap"


def analyze_similarity_chunk(db, playlist_ids: list[int], start: int, end: int) -> int:
    """Persist overlap pairs for a few playlists; safe to retry as one job step."""
    if start >= end or not playlist_ids:
        return 0
    profiles = {playlist_id: set() for playlist_id in playlist_ids}
    rows = db.execute(
        "SELECT playlist_id, track_id FROM playlist_item WHERE playlist_id = ANY(?) AND track_id IS NOT NULL",
        (playlist_ids,),
    ).fetchall()
    for row in rows:
        profiles[row["playlist_id"]].add(row["track_id"])

    pair_rows = []
    for left_index in range(start, end):
        left_id = playlist_ids[left_index]
        left_tracks = profiles[left_id]
        for right_id in playlist_ids[left_index + 1:]:
            right_tracks = profiles[right_id]
            shared = len(left_tracks & right_tracks)
            # The dashboard only presents pairs with overlap. Avoid storing thousands
            # of "distinct" rows that can never be displayed.
            if not shared:
                continue
            overlap = jaccard(left_tracks, right_tracks)
            containment = shared / min(len(left_tracks), len(right_tracks)) if left_tracks and right_tracks else 0.0
            pair_rows.append((
                left_id,
                right_id,
                shared,
                overlap,
                containment,
                0.0,
                0.0,
                _classification(overlap, containment),
            ))
    if pair_rows:
        db.executemany(
            "INSERT INTO playlist_pair(playlist_a_id,playlist_b_id,shared_tracks,jaccard,containment,artist_similarity,year_similarity,classification) VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(playlist_a_id,playlist_b_id) DO UPDATE SET shared_tracks=EXCLUDED.shared_tracks,jaccard=EXCLUDED.jaccard,containment=EXCLUDED.containment,artist_similarity=EXCLUDED.artist_similarity,year_similarity=EXCLUDED.year_similarity,classification=EXCLUDED.classification",
            pair_rows,
        )
    return len(pair_rows)
