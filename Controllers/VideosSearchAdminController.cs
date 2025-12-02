using Microsoft.AspNetCore.Mvc;
using YouTubeDataAPI.Services;

namespace YouTubeDataAPI.Controllers;

[ApiController]
[Route("api/search/videos/admin")]
public class VideosSearchAdminController : ControllerBase
{
    private readonly VideosService _mongo;
    private readonly VideosSearchService _search;

    public VideosSearchAdminController(VideosService mongo, VideosSearchService search)
    {
        _mongo = mongo;
        _search = search;
    }

    // One-time or occasional reindex
    [HttpPost("reindex")]
    public async Task<object> ReindexAll()
    {
        var videos = await _mongo.GetAllAsync();
        var indexed = 0;

        foreach (var v in videos)
        {
            await _search.IndexVideoAsync(v);
            indexed++;
        }

        return new { indexed };
    }

    // Quick sanity check – how many docs are in ES?
    [HttpGet("count")]
    public async Task<object> Count()
    {
        var count = await _search.CountAsync();
        return new { count };
    }
}
