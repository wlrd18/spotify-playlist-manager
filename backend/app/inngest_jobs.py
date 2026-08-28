"""Durable background jobs run by Inngest in production."""

import os
from functools import partial

import inngest


inngest_client = inngest.Inngest(
    app_id="spotify-playlist-intelligence",
    # The Vercel integration can expose the Git branch name as INNGEST_ENV.
    # This single-user app uses Inngest's visible Production environment instead.
    env="production",
    # Local development has no Inngest keys. Production enables signature checks
    # when the event key is present in Vercel's protected environment variables.
    is_production=bool(os.getenv("INNGEST_EVENT_KEY")),
)


def configured() -> bool:
    return bool(os.getenv("INNGEST_EVENT_KEY") and os.getenv("INNGEST_SIGNING_KEY"))


async def queue_sync(job_id: str) -> None:
    # The job ID is the only event data. Spotify tokens never leave our database.
    await inngest_client.send(
        inngest.Event(name="spotify/playlists.sync", data={"job_id": job_id}, id=job_id)
    )


@inngest_client.create_function(
    fn_id="sync-spotify-playlists",
    trigger=inngest.TriggerEvent(event="spotify/playlists.sync"),
)
async def sync_spotify_playlists(ctx: inngest.Context) -> dict[str, str]:
    job_id = ctx.event.data.get("job_id")
    if not isinstance(job_id, str) or len(job_id) > 64:
        raise ValueError("Invalid sync job")

    # Import here to avoid a circular import while FastAPI registers this function.
    from .main import finalize_sync, import_playlist_batch, prepare_sync

    prepared = await ctx.step.run("prepare-sync", partial(prepare_sync, job_id))
    results = []
    playlist_ids = prepared["playlist_ids"]
    for start in range(0, len(playlist_ids), 3):
        batch = playlist_ids[start:start + 3]
        batch_results = await ctx.step.run(
            f"import-playlists-{start // 3 + 1}",
            partial(import_playlist_batch, job_id, batch),
        )
        results.extend(batch_results)
    await ctx.step.run("analyze-playlists", partial(finalize_sync, job_id, prepared["owner_id"], results))
    return {"job_id": job_id, "status": "completed"}
