using MongoDB.Driver;
using YouTubeDataAPI.Models;

namespace YouTubeDataAPI.Services;

public class VideosService
{
    private readonly IMongoCollection<Video> _collection;

    public VideosService(IMongoDatabase db)
    {
        _collection = db.GetCollection<Video>("YouTubeData");
    }

    public async Task<List<Video>> GetAllAsync()
    {
        var count = await _collection.CountDocumentsAsync(_ => true);
        Console.WriteLine("VIDEO COUNT = " + count);

        // IMPORTANT: no Limit(1) here
        return await _collection.Find(_ => true).ToListAsync();
    }

    public async Task<Video?> GetFirstAsync() =>
        await _collection.Find(_ => true).FirstOrDefaultAsync();

    public async Task CreateAsync(Video video) =>
        await _collection.InsertOneAsync(video);
}
