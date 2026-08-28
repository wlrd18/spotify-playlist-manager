from app.analysis import detect_outliers, entropy_concentration, genre_breakdown, jaccard, normalize


def test_normalize_removes_common_version_suffix():
    assert normalize("Song Name (2011 Remastered)") == "song name"


def test_jaccard():
    assert jaccard({1, 2, 3}, {2, 3, 4}) == 0.5


def test_concentration_is_higher_for_focused_distribution():
    assert entropy_concentration({"a": 9, "b": 1}) > entropy_concentration({"a": 5, "b": 5})


def test_genre_breakdown_counts_tracks_not_tags():
    rows = [{"genre": "pop", "tracks": 3}, {"genre": "indie", "tracks": 1}]
    assert genre_breakdown(rows, 4) == [{"name": "pop", "tracks": 3, "percent": 75}, {"name": "indie", "tracks": 1, "percent": 25}]


def test_outlier_in_a_focused_artist_playlist_is_flagged():
    tracks = [{"artist_id": 1, "genres": ["pop"]} for _ in range(5)] + [{"artist_id": 2, "genres": ["latin"]}]
    detect_outliers(tracks)
    assert not any(track.get("outlier") for track in tracks[:5])
    assert tracks[-1]["outlier"] is True
