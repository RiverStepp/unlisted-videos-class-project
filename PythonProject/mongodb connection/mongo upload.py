import json
from pymongo import MongoClient, ASCENDING
from pymongo.errors import DuplicateKeyError
from yt_search import YTSearch

class Mongo:
    """
    killed activation for ui and elastic search. needed to cut ties with mongo db to not over inflate
    """
    __inti__(self):
        self.MONGO_URI = "mongodb://localhost:27017"
        self.MONGO_DB_NAME = "YouTubeDB"
        self.MONGO_COLLECTION_NAME = "UnlistedVideoMetadata"

    def run(self):
        # Mongo
        mongo_client = MongoClient(self.MONGO_URI)
        mongo_db = mongo_client[self.MONGO_DB_NAME]
        coll = mongo_db[self.MONGO_COLLECTION_NAME]
        record = json.load(YTSearch.fetch_random_video_metadata())
        coll.create_index([("VideoId", ASCENDING)], unique=True)
        if record is None:
            continue
        doc = {
                "YtMetadata": record
                }

        try:
                    coll.insert_one(doc)
                    print(f"[INSERT] Saved metadata for {video_id}")
        except DuplicateKeyError:
                print(f"[SKIP] Duplicate VideoId in Mongo (already inserted): {video_id}")
            continue

        finally:
            mongo_client.close()