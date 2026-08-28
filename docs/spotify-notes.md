# Spotify notes

This project is read-only. Spotify's current policy restricts analyzing Spotify content and it restricts long-lived caching. Treat this app as a personal technical prototype until the intended use has been approved.

The importer does not require audio features, audio analysis, related artists, or recommendations. Those endpoints have restricted availability for newer Spotify applications. It does request primary-artist metadata to create a genre breakdown, but Spotify documents artist genre tags as deprecated and they may be absent or broad. A playlist can appear in the current user's playlist list but deny access to its items. The app records that case as unreadable and continues.

Official references:

- https://developer.spotify.com/policy
- https://developer.spotify.com/terms
- https://developer.spotify.com/documentation/web-api/reference/get-a-list-of-current-users-playlists
- https://developer.spotify.com/documentation/web-api/reference/get-playlists-items
