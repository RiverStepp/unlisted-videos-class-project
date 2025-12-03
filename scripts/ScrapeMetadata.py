#!/usr/bin/env python3
import json
import urllib.parse
from datetime import datetime

import pyodbc
import yt_dlp
from yt_dlp.utils import DownloadError
from pymongo import MongoClient, ASCENDING
from pymongo.errors import DuplicateKeyError

# ---------------- CONFIG ----------------

# SQL Server connection string
SQL_CONN_STR = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=localhost;"
    "DATABASE=YouTubeDB;"
    "Trusted_Connection=yes;"
)

# MongoDB (localhost)
MONGO_URI = "mongodb://localhost:27017"
MONGO_DB_NAME = "YouTubeDB"
MONGO_COLLECTION_NAME = "UnlistedVideoMetadata"

# Same rate limit as your original scraper: 3 MB/s
RATE_LIMIT_BYTES_PER_SEC = 3 * 1024 * 1024  # 3 MB/s


# ---------------- QUIET LOGGER ----------------

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

COMMON_YTDLP_OPTS = {
    "quiet": True,
    "no_warnings": True,
    "skip_download": True,          # metadata only, no media files
    "ignoreerrors": True,
    "logger": QUIET_LOGGER,
    "cachedir": False,
    "ratelimit": RATE_LIMIT_BYTES_PER_SEC,
    "socket_timeout": 30,
}


# ---------------- HELPERS ----------------

def sanitize_for_mongo(obj):
    """
    Convert any Python object (including dataclasses, datetime.date, etc.)
    into something MongoDB will accept by JSON round-tripping.

    This:
      - Keeps keys the same
      - Converts non-JSON-safe values to strings
    """
    return json.loads(json.dumps(obj, default=str))


def normalize_video_url(video_id: str, video_url: str | None) -> str:
    if video_url and video_url.startswith("http"):
        return video_url
    if video_id:
        return f"https://www.youtube.com/watch?v={urllib.parse.quote_plus(video_id)}"
    return video_url or ""


def fetch_video_metadata(video_url: str) -> dict | None:
    ydl_opts = dict(COMMON_YTDLP_OPTS)
    print(f"[FETCH] {video_url}")
    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(video_url, download=False)
    except DownloadError:
        print(f"[WARN] Failed to fetch metadata for {video_url}, skipping.")
        return None

    if not isinstance(info, dict):
        return None

    # Sanitize yt-dlp info dict for Mongo
    return sanitize_for_mongo(info)


def load_unlisted_videos_from_sql(cursor):
    """
    Yield rows of unlisted videos from dbo.UnlistedVideos.
    """
    cursor.execute(
        """
SELECT
    VideoId,
    VideoTitle,
    Availability,
    PlaylistId,
    PlaylistTitle,
    PlaylistUrl,
    VideoUrl,
    Channel,
    ChannelId,
    Uploader,
    DurationSeconds,
    ViewCount,
    LikeCount,
    UploadDate,
    AddedAt
FROM dbo.UnlistedVideos
WHERE Availability = 'unlisted';
        """
    )
    columns = [col[0] for col in cursor.description]
    while True:
        rows = cursor.fetchmany(500)
        if not rows:
            break
        for row in rows:
            yield dict(zip(columns, row))


# ---------------- MAIN ----------------

def main():
    # Mongo
    mongo_client = MongoClient(MONGO_URI)
    mongo_db = mongo_client[MONGO_DB_NAME]
    coll = mongo_db[MONGO_COLLECTION_NAME]

    # Unique index on VideoId so we don't duplicate
    coll.create_index([("VideoId", ASCENDING)], unique=True)

    # SQL
    sql_conn = pyodbc.connect(SQL_CONN_STR)
    sql_cursor = sql_conn.cursor()

    try:
        for row in load_unlisted_videos_from_sql(sql_cursor):
            video_id = row.get("VideoId")
            video_url = normalize_video_url(video_id, row.get("VideoUrl"))

            if not video_id or not video_url:
                print(f"[SKIP] Missing VideoId or URL for row: {row}")
                continue

            # If you want to skip already-fetched ones, you can uncomment this:
            # if coll.find_one({"VideoId": video_id}, {"_id": 1}):
            #     print(f"[SKIP] Already in Mongo: {video_id}")
            #     continue

            metadata = fetch_video_metadata(video_url)
            if metadata is None:
                continue

            # Sanitize the SQL row for Mongo as well (dates -> strings, etc.)
            sql_data_clean = sanitize_for_mongo(row)

            doc = {
                "VideoId": video_id,
                "VideoUrl": video_url,
                "SqlData": sql_data_clean,
                "YtMetadata": metadata,
            }

            # Immediate write; no "update" semantics, insert only.
            try:
                coll.insert_one(doc)
                print(f"[INSERT] Saved metadata for {video_id}")
            except DuplicateKeyError:
                # In case you re-run the script later and some docs already exist
                print(f"[SKIP] Duplicate VideoId in Mongo (already inserted): {video_id}")
                continue

    finally:
        sql_cursor.close()
        sql_conn.close()
        mongo_client.close()


if __name__ == "__main__":
    main()
