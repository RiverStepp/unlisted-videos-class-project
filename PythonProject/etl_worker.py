import json
import pyodbc
#import ijson
from yt_search import YTSearch
from metrics import CollectMetrics


class ETLWorker:
 # ___________________________________ETL START___________________________________________________________________________
    def events(self, error_msg: str, values: list):
        path = r"C:\Users\carve\OneDrive\Documents\computer doc\'25 zFall\PythonProject\events\events.txt"
        with open(path, "a", encoding="utf-8") as event:
            event.write(error_msg + "\n")
            event.write(f"{values}\n")

    def insert_row(self, value_dict: dict, columns: list[str], table_name: str, cursor):
        col_list = ", ".join(columns)
        placeholders = ", ".join(["?"] * len(columns))
        sql = f"INSERT INTO {table_name} ({col_list}) VALUES ({placeholders})"
        values = [value_dict[col] for col in columns]
        #print(table_name," :   ",values)
        try:
            cursor.execute(sql, values)
        except pyodbc.IntegrityError as e:
            error_msg = str(e)
            if "2627" in error_msg:# or "2601" in error_msg:
                self.events(error_msg, [])
                #print(".")
            else:
                self.events(error_msg, values)
                raise e
        return cursor

    def to_bit(self, transform: bool) -> int:
        if transform:
            return 1
        else:
            return 0

# ___________________________________PARSE JSON/ETL___________________________________________________________________________
    def extract_channel_data(self, record: dict, cursor, columns: list, table: str):
        yt_meta = record.get('YtMetadata', {})
        row = {
            'C_ID': yt_meta.get('channel_id') or None,
            'C_Name': yt_meta.get('channel') or None,
            'C_URL': yt_meta.get('channel_url') or None,
            'C_Uploader': yt_meta.get('uploader') or None
        }
        self.insert_row(row, columns, table, cursor)

    def extract_playlist_data(self, record: dict, cursor, columns: list, table: str):
        yt_meta = record.get('YtMetadata', {})
        row = {
            'P_ID': record.get('PlaylistId') or None,
            'P_Title': record.get('PlaylistTitle') or None,
            'P_URL': record.get('PlaylistUrl') or None,
            'P_C_ID': yt_meta.get('channel_id') or None
        }
        self.insert_row(row, columns, table, cursor)

    def extract_video_data(self, record: dict, cursor, columns: list, table: str):
        yt_meta = record.get('YtMetadata', {})
        row = {
            'V_ID': yt_meta.get('id') or None,
            'V_Title': yt_meta.get('title') or None,
            'V_URL': record.get('VideoUrl') or None,
            'V_P_ID': record.get('PlaylistId') or None,
            'V_Duration': yt_meta.get('duration') or None,
            'V_Views': yt_meta.get('view_count') or None,
            'V_Likes': yt_meta.get('like_count') or None,
            'V_UploadDate': yt_meta.get('upload_date') or None,
            'V_C_ID': yt_meta.get('channel_id') or None,
            'V_Description': yt_meta.get('description') or None,
            'V_Embed': self.to_bit(yt_meta.get('playable_in_embed'))
        }
        self.insert_row(row, columns, table, cursor)


    def upsert_category(self, cursor, category: str) -> int:
        sql = """
        MERGE Category AS target
        USING (SELECT ? AS CT_Category) AS src
            ON target.CT_Category = src.CT_Category
        WHEN NOT MATCHED THEN
            INSERT (CT_Category)
            VALUES (src.CT_Category)
        OUTPUT inserted.CT_ID, $action;
        """
        cursor.execute(sql, category)
        row = cursor.fetchone()
        if row is not None:
            return row[0]
        cursor.execute("SELECT CT_ID FROM Category WHERE CT_Category = ?", category)
        existing = cursor.fetchone()
        if existing:
            return existing[0]

    def insert_video_category(self, cursor, video_id: str, category_id: int):
        sql = """
        INSERT INTO VideoCategoryJunc (VC_V, VC_CT)
        VALUES (?, ?);
        """
        cursor.execute(sql, (video_id, category_id))

    def extract_category_data(self, record: dict, cursor):
        yt_meta = record.get('YtMetadata', {})
        categories = yt_meta.get("categories") or []
        video_id = yt_meta.get("id")
        for category in categories:
            if not isinstance(category, str) or not category.strip():
                continue
            category_clean = category.strip()
            category_id = self.upsert_category(cursor, category_clean)
            self.insert_video_category(cursor, video_id, category_id)

    def extract_tags_data(self, record: dict, cursor, columns: list, table: str):
        yt_meta = record.get('YtMetadata', {})
        video_id = yt_meta.get("id")
        tags = yt_meta.get('tags') or []
        for tag in tags:
            row_dict = {
                'Tag': tag,
                'VT_V': video_id
            }
            self.insert_row(row_dict, columns, table, cursor)
# ___________________________________END OF ETL___________________________________________________________________________

# ___________________________________METRIC LOG WRITING___________________________________________________________________________
    def write_metrics(self, cursor, path="metrics_log.json"):
        metrics = {}

        # Total counts
        cursor.execute("SELECT COUNT(*) FROM Video")
        metrics["video_count"] = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM Channel")
        metrics["channel_count"] = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM Playlist")
        metrics["playlist_count"] = cursor.fetchone()[0]

        # Average values
        cursor.execute("SELECT AVG(V_Duration) FROM Video")
        metrics["avg_video_duration"] = cursor.fetchone()[0]
        cursor.execute("SELECT AVG(V_Views) FROM Video")
        metrics["avg_video_views"] = cursor.fetchone()[0]
        cursor.execute("SELECT AVG(V_Likes) FROM Video")
        metrics["avg_video_likes"] = cursor.fetchone()[0]

        # Playlist metrics
        cursor.execute("""
            SELECT P_Title, SUM(ISNULL(V_Duration,0)) AS total_duration
            FROM Playlist
            JOIN Video ON Playlist.P_ID = Video.V_P_ID
            GROUP BY P_Title
            ORDER BY total_duration DESC
        """)
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
        cursor.execute("""
            SELECT V_Title, V_Views
            FROM Video
            ORDER BY V_Views DESC
        """)
        top_by_views = cursor.fetchone()
        metrics["top_video_by_views"] = {"V_Title": top_by_views[0],
                                         "V_Views": top_by_views[1]} if top_by_views else None

        # Top video by likes
        cursor.execute("""
            SELECT V_Title, V_Likes
            FROM Video
            ORDER BY V_Likes DESC
        """)
        top_by_likes = cursor.fetchone()
        metrics["top_video_by_likes"] = {"V_Title": top_by_likes[0],
                                         "V_Likes": top_by_likes[1]} if top_by_likes else None

        # Top video by duration
        cursor.execute("""
            SELECT V_Title, V_Duration
            FROM Video
            ORDER BY V_Duration DESC
        """)
        top_by_duration = cursor.fetchone()
        metrics["top_video_by_duration"] = {"V_Title": top_by_duration[0],
                                            "V_Duration": top_by_duration[1]} if top_by_duration else None

        # Top channel by number of videos
        cursor.execute("""
            SELECT C_Name, COUNT(Video.V_ID) AS video_count
            FROM Channel
            JOIN Video ON Channel.C_ID = Video.V_C_ID
            GROUP BY C_Name
            ORDER BY video_count DESC
        """)
        top_channel = cursor.fetchone()
        metrics["top_channel_by_videos"] = {"C_Name": top_channel[0],
                                            "video_count": top_channel[1]} if top_channel else None
        with open(path, "w", encoding="utf-8") as f:
            json.dump(metrics, f, indent=4)
#___________________________________END OF LOG WRITING___________________________________________________________________________

#___________________________________RUN POINT___________________________________________________________________________

    def process_record(self, record: dict, cursor):
        tables = {
            'Channel': {'func': self.extract_channel_data, 'columns': ["C_ID", "C_Name", "C_URL", "C_Uploader"]},
            'Playlist': {'func': self.extract_playlist_data, 'columns': ["P_ID", "P_Title", "P_URL", "P_C_ID"]},
            'Video': {'func': self.extract_video_data, 'columns': ["V_ID", "V_Title", "V_URL", "V_P_ID", "V_Duration", "V_Views", "V_Likes", "V_UploadDate", "V_C_ID", "V_Description", "V_Embed"]},
            'Tags': {'func': self.extract_tags_data, 'columns': ["Tag", "VT_V"]}
        }
        #for table, info in tables.items():
            #extraction_function = info['func']
            #extraction_function(record, cursor, info['columns'], table)
        self.extract_channel_data(record, cursor, tables['Channel']['columns'], 'Channel')
        cursor.connection.commit()
        self.extract_playlist_data(record, cursor, tables['Playlist']['columns'], 'Playlist')
        cursor.connection.commit()
        self.extract_video_data(record, cursor, tables['Video']['columns'], 'Video')
        cursor.connection.commit()
        self.extract_tags_data(record, cursor, tables['Tags']['columns'], 'Tags')
        cursor.connection.commit()
        self.extract_category_data(record, cursor)
        cursor.connection.commit()

    def run(self):
        conn = pyodbc.connect('Driver={ODBC Driver 17 for SQL Server};'
                              'Server=CARVERLENYOGA7I;'
                              'Database=BD_Project;'
                              'Trusted_Connection=yes;')
        cursor = conn.cursor()
        processed_count = 0
        search = YTSearch()
        while True:
            record = search.fetch_random_video_metadata() # Run the API connection code
            if record:
                self.process_record(record, cursor)
                processed_count += 1
                if processed_count % 5 == 0:
                    self.write_metrics(cursor, r"C:\Users\carve\OneDrive\Documents\computer doc\'25 zFall\PythonProject\logs\metrics_log.json")
                    #CollectMetrics.write_metrics(cursor, 1, r"C:\Users\carve\OneDrive\Documents\computer doc\'25 zFall\PythonProject\logs\metrics_log.json")


#For file reading
#    def run(self):
#        conn = pyodbc.connect('Driver={ODBC Driver 17 for SQL Server};'
#                              'Server=CARVERLENYOGA7I;'
#                              'Database=BD_Project;'
#                              'Trusted_Connection=yes;')
#        cursor = conn.cursor()
#        processed_count = 0
#        path = r"C:\Users\carve\OneDrive\Documents\computer doc\'25 zFall\DSA 469 Big Data\YouTubeDB.UnlistedVideoMetadata2 (1).json"
#        with open(path, "r", encoding='utf-8') as f:
#            #data = json.load(f)
#            for record in ijson.items(f, 'item'):
#                if processed_count == 0:
#                    print("collected json file")
#                self.process_record(record, cursor)
#                processed_count += 1
#                if processed_count % 50 == 0:
#                    self.write_metrics(cursor)