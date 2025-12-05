import json

class CollectMetrics:
        def __main__(self, cursor, activate: int=0, path="metrics_log.json"):
            metrics = {}

            # Total counts
            path = r"C:\Users\carve\OneDrive\Documents\computer doc\'25 zFall\PythonProject\database\video_count.sql"
            with open(path, 'r', encoding='UTF-8') as sql:
                SQL = str(sql)
                cursor.execute(SQL)
            metrics["video_count"] = cursor.fetchone()[0]
            path = r"C:\Users\carve\OneDrive\Documents\computer doc\'25 zFall\PythonProject\database\channel_count.sql"
            with open(path, 'r', encoding='UTF-8') as sql:
                SQL = str(sql)
                cursor.execute(SQL)
            metrics["channel_count"] = cursor.fetchone()[0]
            path = r"C:\Users\carve\OneDrive\Documents\computer doc\'25 zFall\PythonProject\database\playlist_count.sql"
            with open(path, 'r', encoding='UTF-8') as sql:
                SQL = str(sql)
                cursor.execute(SQL)
            metrics["playlist_count"] = cursor.fetchone()[0]

            # Average values
            path = r"C:\Users\carve\OneDrive\Documents\computer doc\'25 zFall\PythonProject\database\ave_video_duration.sql"
            with open(path, 'r', encoding='UTF-8') as sql:
                SQL = str(sql)
                cursor.execute(SQL)
            metrics["avg_video_duration"] = cursor.fetchone()[0]
            path = r"C:\Users\carve\OneDrive\Documents\computer doc\'25 zFall\PythonProject\database\ave_video_views.sql"
            with open(path, 'r', encoding='UTF-8') as sql:
                SQL = str(sql)
                cursor.execute(SQL)
            metrics["avg_video_views"] = cursor.fetchone()[0]
            path = r"C:\Users\carve\OneDrive\Documents\computer doc\'25 zFall\PythonProject\database\ave_video_likes.sql"
            with open(path, 'r', encoding='UTF-8') as sql:
                SQL = str(sql)
                cursor.execute(SQL)
            metrics["avg_video_likes"] = cursor.fetchone()[0]

            # Playlist metrics
            path = r"C:\Users\carve\OneDrive\Documents\computer doc\'25 zFall\PythonProject\database\longest_playlist.sql"
            with open(path, 'r', encoding='UTF-8') as sql:
                SQL = str(sql)
                cursor.execute(SQL)
            playlist_durations = cursor.fetchall()
            if playlist_durations:
                metrics["longest_playlist"] = {
                    "P_Title": playlist_durations[0][0],
                    "total_duration": playlist_durations[0][1]
                }
                # Average playlist length
                avg_playlist_len = float(sum(row[1] for row in playlist_durations) / len(playlist_durations))
                metrics["avg_playlist_duration"] = avg_playlist_len
            else:
                metrics["longest_playlist"] = {"P_Title": None, "total_duration": 0}
                metrics["avg_playlist_duration"] = 0

            # Top video by views
            path = r"C:\Users\carve\OneDrive\Documents\computer doc\'25 zFall\PythonProject\database\top_video_by_views.sql"
            with open(path, 'r', encoding='UTF-8') as sql:
                SQL = str(sql)
                cursor.execute(SQL)
            top_by_views = cursor.fetchone()
            metrics["top_video_by_views"] = {"V_Title": top_by_views[0],
                                             "V_Views": top_by_views[1]} if top_by_views else None

            # Top video by likes
            path = r"C:\Users\carve\OneDrive\Documents\computer doc\'25 zFall\PythonProject\database\top_video_by_likes.sql"
            with open(path, 'r', encoding='UTF-8') as sql:
                SQL = str(sql)
                cursor.execute(SQL)
            top_by_likes = cursor.fetchone()
            metrics["top_video_by_likes"] = {"V_Title": top_by_likes[0],
                                             "V_Likes": top_by_likes[1]} if top_by_likes else None

            # Top video by duration
            path = r"C:\Users\carve\OneDrive\Documents\computer doc\'25 zFall\PythonProject\database\top_video_by_duration.sql"
            with open(path, 'r', encoding='UTF-8') as sql:
                SQL = str(sql)
                cursor.execute(SQL)
            top_by_duration = cursor.fetchone()
            metrics["top_video_by_duration"] = {"V_Title": top_by_duration[0],
                                                "V_Duration": top_by_duration[1]} if top_by_duration else None

            # Top channel by number of videos
            path = r"C:\Users\carve\OneDrive\Documents\computer doc\'25 zFall\PythonProject\database\top_channel_by_video.sql"
            with open(path, 'r', encoding='UTF-8') as sql:
                SQL = str(sql)
                cursor.execute(SQL)
            top_channel = cursor.fetchone()
            metrics["top_channel_by_videos"] = {"C_Name": top_channel[0],
                                                "video_count": top_channel[1]} if top_channel else None
            with open(path, "w", encoding="utf-8") as f:
                json.dump(metrics, f, indent=4)
            activate -= 1

        if name == __main__():
            main()