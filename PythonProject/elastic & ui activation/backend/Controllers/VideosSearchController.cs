using Microsoft.AspNetCore.Mvc;
using YouTubeDataAPI.Models;
using YouTubeDataAPI.Services;

namespace YouTubeDataAPI.Controllers;

[ApiController]
[Route("api/search/videos")]
public class VideosSearchController : ControllerBase
{
    private readonly VideosSearchService _svc;

    public VideosSearchController(VideosSearchService svc)
    {
        _svc = svc;
    }

    [HttpGet]
    public async Task<SearchPageResult> Search(
        [FromQuery] string q,
        [FromQuery] int page = 1,
        [FromQuery] int pageSize = 12)
    {
        if (string.IsNullOrWhiteSpace(q))
        {
            return new SearchPageResult
            {
                Page = page,
                PageSize = pageSize,
                Total = 0,
                TotalPages = 0,
                Results = new List<Video>()
            };
        }

        return await _svc.SearchAsync(q, page, pageSize);
    }
}
