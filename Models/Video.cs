using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;

namespace YouTubeDataAPI.Models;

public class Video
{
    [BsonId]
    [BsonRepresentation(BsonType.ObjectId)]
    public string Id { get; set; } = null!;

    public string VideoId { get; set; } = null!;
    public string VideoUrl { get; set; } = null!;

    public SqlMetadata? SqlData { get; set; }
    public YtMetadata? YtMetadata { get; set; }
}

[BsonIgnoreExtraElements] // ignore all the other SqlData fields in Mongo
public class SqlMetadata
{
    public string VideoId { get; set; } = null!;
    public string VideoTitle { get; set; } = null!;
    public string Uploader { get; set; } = null!;
    public string Channel { get; set; } = null!;

    // optional but likely useful for search:
    public string? Availability { get; set; }          // "Availability" in Mongo
    public string? PlaylistTitle { get; set; }         // "PlaylistTitle" in Mongo
}

[BsonIgnoreExtraElements] // YtMetadata in Mongo has tons of extra stuff
public class YtMetadata
{
    [BsonElement("title")]
    public string? Title { get; set; }

    [BsonElement("description")]
    public string? Description { get; set; }

    [BsonElement("tags")]
    public List<string>? Tags { get; set; }
}
