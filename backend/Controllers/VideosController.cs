using Microsoft.AspNetCore.Mvc;
using YouTubeDataAPI.Services;
using YouTubeDataAPI.Models;

namespace YouTubeDataAPI.Controllers;

[ApiController]
[Route("api/videos")]
public class VideosController : ControllerBase
{
    private readonly VideosService _svc;

    public VideosController(VideosService svc) => _svc = svc;

    [HttpGet]
    public Task<List<Video>> GetAll() => _svc.GetAllAsync();

    [HttpGet("first")]
    public Task<Video?> GetFirst() => _svc.GetFirstAsync();
}
