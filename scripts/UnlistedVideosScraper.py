#!/usr/bin/env python3
import random
import urllib.parse
from datetime import datetime

import pyodbc
import yt_dlp
from wordfreq import top_n_list
from yt_dlp.utils import DownloadError

# ---------------- CONFIG ----------------

SQL_CONN_STR = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=localhost;"
    "DATABASE=YouTubeDB;"
    "Trusted_Connection=yes;"
)

RATE_LIMIT_BYTES_PER_SEC = 3 * 1024 * 1024  # 3 MB/s

# playlists whose (estimated) last update < this -> stop for that search
# (currently NOT used; checks are disabled)
MIN_PLAYLIST_UPDATE_DATE = datetime(2017, 1, 1).date()

# random word source for search queries (kept, but not used now)
WORD_LIST = top_n_list("en", 50000)

# HARD-CODED SEARCH TERMS (edit these to whatever you want)
HARDCODED_QUERIES = [
    "kohi"
]

# RESUME AFTER THIS PLAYLIST (skip everything before and this one itself)
SKIP_UNTIL_PLAYLIST_URL = ''

# YouTube: Type = Playlist, Sort by = Upload date
SEARCH_RESULTS_PLAYLIST_FILTER = "CAISAhAD"

# global progress counter
TOTAL_VIDEOS_PROCESSED = 0


# ------------- QUIET LOGGER -------------

class QuietLogger:
    def debug(self, msg):
        pass

    def info(self, msg):
        pass

    def warning(self, msg):
        pass

    def error(self, msg):
        pass


QUIET_LOGGER = QuietLogger()

# Common yt-dlp options:
# - skip_download=True -> ONLY metadata, no video/audio files
# - ratelimit -> cap bandwidth at 3 MB/s
COMMON_YTDLP_OPTS = {
    "quiet": True,
    "no_warnings": True,
    "skip_download": True,          # <-- metadata only, no actual video download
    "ignoreerrors": True,
    "logger": QUIET_LOGGER,
    "cachedir": False,
    "ratelimit": RATE_LIMIT_BYTES_PER_SEC,
    "socket_timeout": 30,           # fail a stuck request after 30s
}


# ------------- RANDOM QUERY (NOT USED NOW) -------------

def random_query() -> str:
    # mostly 1–2 word queries, sometimes 3
    n = random.choice([1, 2, 2, 3])
    words = random.choices(WORD_LIST, k=n)
    return " ".join(words)


# ------------- YT-DLP HELPERS -----------

def make_search_url(query: str) -> str:
    encoded_q = urllib.parse.quote_plus(query)
    return (
        f"https://www.youtube.com/results?"
        f"search_query={encoded_q}&sp={SEARCH_RESULTS_PLAYLIST_FILTER}"
    )


def yt_search_playlists(query: str):
    """Search YouTube for playlist search results for the given query."""
    ydl_opts = dict(COMMON_YTDLP_OPTS)
    ydl_opts["extract_flat"] = True  # search results only

    search_url = make_search_url(query)
    print(f"[SEARCH] {query!r} -> {search_url}")

    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(search_url, download=False)
    except DownloadError:
        print("[WARN] Search request failed, skipping query.")
        return []

    if not isinstance(info, dict):
        return []

    entries = list(info.get("entries") or [])
    # CAISAhAD already gives newest-first; we KEEP that order.
    print(f"[INFO] Got {len(entries)} playlist search results.")
    return entries


def normalize_playlist_url(entry: dict) -> str:
    url = entry.get("url") or entry.get("webpage_url") or ""
    if url.startswith("http"):
        return url
    return f"https://www.youtube.com/playlist?list={url}"


def enumerate_unlisted_from_playlist(playlist_url: str):
    """
    Fetch playlist, estimate last_update from entries' upload_date,
    and return (last_update, [unlisted_records]).

    The last_update check against MIN_PLAYLIST_UPDATE_DATE is DISABLED for now.
    """
    global TOTAL_VIDEOS_PROCESSED

    ydl_opts = dict(COMMON_YTDLP_OPTS)

    print(f"[INFO] Fetching playlist metadata: {playlist_url}")
    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            pl = ydl.extract_info(playlist_url, download=False)
    except DownloadError:
        print("[WARN] Playlist fetch failed, skipping playlist.")
        return None, []

    if not isinstance(pl, dict):
        return None, []

    pl_title = pl.get("title")
    pl_id = pl.get("id")
    pl_webpage = pl.get("webpage_url") or playlist_url
    entries = pl.get("entries") or []

    # ---- estimate "last updated" from entries' upload_date ----
    last_update = None
    for v in entries:
        if not isinstance(v, dict):
            continue
        ud = v.get("upload_date")  # "YYYYMMDD"
        if not ud or len(ud) != 8:
            continue
        try:
            d = datetime.strptime(ud, "%Y%m%d").date()
        except ValueError:
            continue
        if last_update is None or d > last_update:
            last_update = d

    # CHECK DISABLED:
    # if last_update is not None and last_update < MIN_PLAYLIST_UPDATE_DATE:
    #     print(
    #         f"[STOP THIS SEARCH] Playlist {pl_id} ({pl_webpage}) "
    #         f"estimated_last_update={last_update} < {MIN_PLAYLIST_UPDATE_DATE}"
    #     )
    #     return last_update, []

    print(
        f"[PLAYLIST] {pl_title!r} ({pl_webpage}) "
        f"estimated_last_update={last_update or 'unknown'}"
    )

    unlisted_records = []

    for v in entries:
        if not isinstance(v, dict):
            continue

        TOTAL_VIDEOS_PROCESSED += 1

        availability = v.get("availability")  # "public", "unlisted", "private", etc.
        video_url = v.get("webpage_url") or (
            f"https://www.youtube.com/watch?v={v.get('id')}"
            if v.get("id")
            else None
        )

        print(
            f"[PROGRESS] total_videos={TOTAL_VIDEOS_PROCESSED} "
            f"playlist={pl_id} video_id={v.get('id')} "
            f"availability={availability} url={video_url}"
        )

        # Skip private
        if availability == "private":
            continue

        is_unlisted_flag = v.get("is_unlisted")

        # Only care about unlisted
        if not (availability == "unlisted" or is_unlisted_flag):
            continue

        upload_date = v.get("upload_date")  # "YYYYMMDD" or None
        upload_date_obj = None
        if upload_date and len(upload_date) == 8:
            try:
                upload_date_obj = datetime.strptime(upload_date, "%Y%m%d").date()
            except ValueError:
                upload_date_obj = None

        record = {
            "VideoId": v.get("id"),
            "VideoTitle": v.get("title"),
            "Availability": availability,
            "PlaylistId": pl_id,
            "PlaylistTitle": pl_title,
            "PlaylistUrl": pl_webpage,
            "VideoUrl": video_url,
            "Channel": v.get("channel"),
            "ChannelId": v.get("channel_id"),
            "Uploader": v.get("uploader"),
            "DurationSeconds": v.get("duration"),
            "ViewCount": v.get("view_count"),
            "LikeCount": v.get("like_count"),
            "UploadDate": upload_date_obj,
        }

        unlisted_records.append(record)

    return last_update, unlisted_records


# ------------- DB HELPERS ----------------

def ensure_table_exists(cursor):
    cursor.execute(
        """
IF NOT EXISTS (
    SELECT 1 FROM sys.tables WHERE name = 'UnlistedVideos' AND schema_id = SCHEMA_ID('dbo')
)
BEGIN
    CREATE TABLE dbo.UnlistedVideos (
        Id              BIGINT IDENTITY(1,1) PRIMARY KEY,
        VideoId         NVARCHAR(64)  NOT NULL,
        VideoTitle      NVARCHAR(1024) NULL,
        Availability    NVARCHAR(32)  NULL,
        PlaylistId      NVARCHAR(64)  NULL,
        PlaylistTitle   NVARCHAR(1024) NULL,
        PlaylistUrl     NVARCHAR(512) NULL,
        VideoUrl        NVARCHAR(512) NULL,
        Channel         NVARCHAR(512) NULL,
        ChannelId       NVARCHAR(128) NULL,
        Uploader        NVARCHAR(512) NULL,
        DurationSeconds INT NULL,
        ViewCount       BIGINT NULL,
        LikeCount       BIGINT NULL,
        UploadDate      DATE NULL,
        AddedAt         DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        CONSTRAINT UQ_UnlistedVideos_VideoPlaylist UNIQUE (VideoId, PlaylistId)
    );
END
        """
    )


def insert_unlisted_video(cursor, record):
    """
    Insert if (VideoId, PlaylistId) not already present.
    Uses WHERE NOT EXISTS plus a unique index to avoid duplicates efficiently.
    """
    cursor.execute(
        """
INSERT INTO dbo.UnlistedVideos (
    VideoId, VideoTitle, Availability,
    PlaylistId, PlaylistTitle, PlaylistUrl,
    VideoUrl, Channel, ChannelId, Uploader,
    DurationSeconds, ViewCount, LikeCount, UploadDate
)
SELECT ?,?,?,?,?,?,?,?,?,?,?,?,?,?
WHERE NOT EXISTS (
    SELECT 1 FROM dbo.UnlistedVideos
    WHERE VideoId = ? AND (PlaylistId = ? OR (PlaylistId IS NULL AND ? IS NULL))
);
        """,
        (
            record["VideoId"],
            record["VideoTitle"],
            record["Availability"],
            record["PlaylistId"],
            record["PlaylistTitle"],
            record["PlaylistUrl"],
            record["VideoUrl"],
            record["Channel"],
            record["ChannelId"],
            record["Uploader"],
            record["DurationSeconds"],
            record["ViewCount"],
            record["LikeCount"],
            record["UploadDate"],
            # WHERE NOT EXISTS parameters
            record["VideoId"],
            record["PlaylistId"],
            record["PlaylistId"],
        ),
    )


# ------------- MAIN LOOP -----------------

def main():
    conn = pyodbc.connect(SQL_CONN_STR)
    conn.autocommit = False           # explicit control over commits
    cursor = conn.cursor()
    ensure_table_exists(cursor)
    conn.commit()

    insert_counter = 0
    skipping_done = False  # have we reached the resume playlist yet?

    try:
        while True:
            # loop over hardcoded queries
            for query in HARDCODED_QUERIES:
                playlists = yt_search_playlists(query)

                # newest playlists first (no reverse)
                for p in playlists:
                    if not isinstance(p, dict):
                        continue

                    pl_url = normalize_playlist_url(p)

                    # --------- RESUME LOGIC: SKIP UNTIL TARGET PLAYLIST ----------
                    if not skipping_done:
                        if pl_url == SKIP_UNTIL_PLAYLIST_URL or SKIP_UNTIL_PLAYLIST_URL == '':
                            print(
                                f"[RESUME] Reached resume point playlist {pl_url}; "
                                f"next playlist will be the first processed."
                            )
                            skipping_done = True
                        else:
                            print(f"[RESUME] Skipping playlist before resume point: {pl_url}")
                        continue
                    # ----------------------------------------------------------------

                    before_playlist_inserts = insert_counter

                    last_update, records = enumerate_unlisted_from_playlist(pl_url)

                    # DATE CHECK DISABLED:
                    # if last_update is not None and last_update < MIN_PLAYLIST_UPDATE_DATE:
                    #     print("[INFO] Hit playlist before 2017; moving to next search term.")
                    #     break  # break out of "for p in playlists"

                    for record in records:
                        if not record.get("VideoId"):
                            continue
                        insert_unlisted_video(cursor, record)
                        insert_counter += 1

                    # commit immediately after finishing this playlist
                    conn.commit()
                    if insert_counter > before_playlist_inserts:
                        print(
                            f"[INFO] Committed playlist {pl_url} "
                            f"(total inserts so far: {insert_counter})"
                        )

    finally:
        try:
            conn.commit()
        except Exception:
            pass
        cursor.close()
        conn.close()


if __name__ == "__main__":
    main()
