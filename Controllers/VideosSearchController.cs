using Microsoft.AspNetCore.Mvc;
using YouTubeDataAPI.Services;

namespace YouTubeDataAPI.Controllers;

[ApiController]
[Route("api/search/videos")]
public class VideosSearchController : ControllerBase
{
    private readonly VideosSearchService _svc;

    public VideosSearchController(VideosSearchService svc) => _svc = svc;

    [HttpGet]
    public async Task<object> Search([FromQuery] string q)
    {
        if (string.IsNullOrWhiteSpace(q))
            return new { results = new List<object>() };

        return await _svc.SearchAsync(q);
    }
}
