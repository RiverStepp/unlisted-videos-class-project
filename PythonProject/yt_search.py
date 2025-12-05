import random
import urllib.parse
from datetime import datetime
import yt_dlp
from yt_dlp.utils import DownloadError
from wordfreq import top_n_list
import json


class YTSearch:
    def __init__(self):
        self.RATE_LIMIT_BYTES_PER_SEC = 3 * 1024 * 1024  # 3 MB/s
        self.WORD_LIST = top_n_list("en", 50000)  # random word list

        self.QUIET_LOGGER = self.QuietLogger()

        self.COMMON_YTDLP_OPTS = {
            "quiet": True,
            "no_warnings": True,
            "skip_download": True,
            "ignoreerrors": True,
            "logger": self.QUIET_LOGGER,
            "cachedir": False,
            "ratelimit": self.RATE_LIMIT_BYTES_PER_SEC,
            "socket_timeout": 30,
        }

    class QuietLogger:
        def debug(self, msg): pass
        def info(self, msg): pass
        def warning(self, msg): pass
        def error(self, msg): pass

    # ---------------- HELPERS ----------------
    def random_query(self):
        n = random.choice([1, 2, 2, 3])
        return " ".join(random.choices(self.WORD_LIST, k=n))

    @staticmethod
    def sanitize_for_json(obj):
        return json.loads(json.dumps(obj, default=str))

    @staticmethod
    def make_playlist_search_url(query: str) -> str:
        encoded_q = urllib.parse.quote_plus(query)
        return f"https://www.youtube.com/results?search_query={encoded_q}&sp=EgIQAw%3D%3D"

    @staticmethod
    def normalize_video_url(video_id, video_url):
        if video_url and video_url.startswith("http"):
            return video_url
        if video_id:
            return f"https://www.youtube.com/watch?v={urllib.parse.quote_plus(video_id)}"
        return video_url or ""

    # ---------------- MAIN FUNCTION ----------------
    def fetch_random_video_metadata(self, max_attempts=10) -> dict | None:
        for attempt in range(max_attempts):
            query = self.random_query()
            search_url = self.make_playlist_search_url(query)

            print(f"[SEARCH] Attempt {attempt+1}/{max_attempts} - query=\"{query}\"")

            try:
                with yt_dlp.YoutubeDL(
                    {**self.COMMON_YTDLP_OPTS, "extract_flat": True}
                ) as ydl:
                    search_results = ydl.extract_info(search_url, download=False)
            except DownloadError:
                continue

            if not isinstance(search_results, dict):
                continue

            playlist_entries = list(search_results.get("entries") or [])
            print(f"[SEARCH] Found {len(playlist_entries)} playlist candidates")

            for playlist in playlist_entries:
                if not isinstance(playlist, dict):
                    continue

                playlist_url = playlist.get("url") or playlist.get("webpage_url")
                if not playlist_url:
                    continue

                try:
                    with yt_dlp.YoutubeDL(
                        {**self.COMMON_YTDLP_OPTS, "extract_flat": True}
                    ) as ydl:
                        pl_data = ydl.extract_info(playlist_url, download=False)
                except DownloadError:
                    continue

                if not isinstance(pl_data, dict):
                    continue

                pl_id = pl_data.get("id")
                pl_title = pl_data.get("title")
                pl_entries = list(pl_data.get("entries") or [])

                print(f"[PLAYLIST] {pl_title} ({len(pl_entries)} videos)")
                for item in pl_entries:
                    if not isinstance(item, dict):
                        continue

                    video_id = item.get("id")
                    if not video_id:
                        continue

                    video_url = self.normalize_video_url(video_id, item.get("url"))
                    try:
                        with yt_dlp.YoutubeDL(self.COMMON_YTDLP_OPTS) as ydl:
                            metadata = ydl.extract_info(video_url, download=False)
                    except DownloadError:
                        continue

                    if not isinstance(metadata, dict):
                        continue

                    upload_date = metadata.get("upload_date")
                    upload_date_obj = None
                    if upload_date and len(upload_date) == 8:
                        try:
                            upload_date_obj = datetime.strptime(upload_date, "%Y%m%d").date()
                        except ValueError:
                            pass

                    record = {
                        "VideoId": metadata.get("id"),
                        "VideoTitle": metadata.get("title"),
                        "Availability": metadata.get("availability"),
                        "VideoUrl": video_url,
                        "Channel": metadata.get("channel"),
                        "ChannelId": metadata.get("channel_id"),
                        "Uploader": metadata.get("uploader"),
                        "DurationSeconds": metadata.get("duration"),
                        "ViewCount": metadata.get("view_count"),
                        "LikeCount": metadata.get("like_count"),
                        "UploadDate": upload_date_obj,
                        "YtMetadata": self.sanitize_for_json(metadata),
                        "PlaylistId": pl_id,
                        "PlaylistTitle": pl_title,
                        "PlaylistUrl": playlist_url,
                        "PlaylistIndex": item.get("playlist_index"),
                    }

                    print(f"[FOUND] {record['VideoTitle']} (Playlist: {pl_title})")
                    return record

        return None