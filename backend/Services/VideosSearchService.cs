using System.Net;
using System.Text;
using System.Text.Json;
using Microsoft.Extensions.Options;
using YouTubeDataAPI.Models;

namespace YouTubeDataAPI.Services;

public class VideosSearchService
{
    private readonly HttpClient _http;
    private readonly string _index;

    public VideosSearchService(HttpClient http, IOptions<ElasticSettings> settings)
    {
        _http = http;
        var cfg = settings.Value;
        _index = cfg.Index;

        if (_http.BaseAddress == null)
        {
            _http.BaseAddress = new Uri(cfg.Url);
        }
    }

    public async Task EnsureIndexExistsAsync()
    {
        using var headResp = await _http.SendAsync(
            new HttpRequestMessage(HttpMethod.Head, $"/{_index}")
        );

        if (headResp.IsSuccessStatusCode)
        {
            Console.WriteLine($"[ES] index '{_index}' already exists");
            return;
        }

        if (headResp.StatusCode != HttpStatusCode.NotFound)
        {
            var body = await headResp.Content.ReadAsStringAsync();
            Console.WriteLine($"[ES] HEAD /{_index} failed: {(int)headResp.StatusCode} {headResp.ReasonPhrase} {body}");
            headResp.EnsureSuccessStatusCode();
        }

        var createBody = new
        {
            mappings = new
            {
                properties = new
                {
                    sqlData = new
                    {
                        properties = new
                        {
                            videoTitle = new { type = "text" },
                            uploader = new { type = "text" },
                            channel = new { type = "text" }
                        }
                    },
                    ytMetadata = new
                    {
                        properties = new
                        {
                            title = new { type = "text" },
                            description = new { type = "text" },
                            tags = new { type = "text" }
                        }
                    }
                }
            }
        };

        var json = JsonSerializer.Serialize(createBody);
        using var createResp = await _http.PutAsync(
            $"/{_index}",
            new StringContent(json, Encoding.UTF8, "application/json")
        );

        var respText = await createResp.Content.ReadAsStringAsync();
        Console.WriteLine($"[ES] PUT /{_index} => {(int)createResp.StatusCode} {createResp.ReasonPhrase} {respText}");
        createResp.EnsureSuccessStatusCode();
    }

    public async Task IndexVideoAsync(Video v)
    {
        var doc = new
        {
            id = v.Id,
            videoId = v.VideoId,
            videoUrl = v.VideoUrl,
            sqlData = v.SqlData == null ? null : new
            {
                videoId = v.SqlData.VideoId,
                videoTitle = v.SqlData.VideoTitle,
                uploader = v.SqlData.Uploader,
                channel = v.SqlData.Channel
            },
            ytMetadata = v.YtMetadata == null ? null : new
            {
                title = v.YtMetadata.Title,
                description = v.YtMetadata.Description,
                tags = v.YtMetadata.Tags
            }
        };

        var json = JsonSerializer.Serialize(doc);
        using var resp = await _http.PutAsync(
            $"/{_index}/_doc/{v.Id}",
            new StringContent(json, Encoding.UTF8, "application/json")
        );

        var body = await resp.Content.ReadAsStringAsync();
        if (!resp.IsSuccessStatusCode)
        {
            Console.WriteLine($"[ES] index {v.Id} failed: {(int)resp.StatusCode} {resp.ReasonPhrase} {body}");
        }
    }

    public async Task<long> CountAsync()
    {
        using var resp = await _http.GetAsync($"/{_index}/_count");
        var text = await resp.Content.ReadAsStringAsync();

        if (!resp.IsSuccessStatusCode)
        {
            Console.WriteLine($"[ES] _count failed: {(int)resp.StatusCode} {resp.ReasonPhrase} {text}");
            return 0;
        }

        using var doc = JsonDocument.Parse(text);
        if (doc.RootElement.TryGetProperty("count", out var countEl) &&
            countEl.ValueKind == JsonValueKind.Number)
        {
            return countEl.GetInt64();
        }

        return 0;
    }

    public async Task<SearchPageResult> SearchAsync(string query, int page, int pageSize)
    {
        if (page < 1) page = 1;
        if (pageSize <= 0) pageSize = 12;
        if (pageSize > 100) pageSize = 100;

        var from = (page - 1) * pageSize;

        var body = new
        {
            from,
            size = pageSize,
            track_total_hits = true,
            query = new
            {
                multi_match = new
                {
                    query,
                    fields = new[]
                    {
                        "sqlData.videoTitle",
                        "ytMetadata.title",
                        "ytMetadata.description",
                        "sqlData.uploader",
                        "sqlData.channel",
                        "ytMetadata.tags"
                    },
                    fuzziness = "AUTO"
                }
            }
        };

        var json = JsonSerializer.Serialize(body);
        using var resp = await _http.PostAsync(
            $"/{_index}/_search",
            new StringContent(json, Encoding.UTF8, "application/json")
        );

        var respJson = await resp.Content.ReadAsStringAsync();
        if (!resp.IsSuccessStatusCode)
        {
            Console.WriteLine($"[ES] _search failed: {(int)resp.StatusCode} {resp.ReasonPhrase} {respJson}");
            resp.EnsureSuccessStatusCode();
        }

        using var doc = JsonDocument.Parse(respJson);
        var hitsRoot = doc.RootElement.GetProperty("hits");

        long total = 0;
        if (hitsRoot.TryGetProperty("total", out var totalProp))
        {
            if (totalProp.ValueKind == JsonValueKind.Number)
                total = totalProp.GetInt64();
            else if (totalProp.ValueKind == JsonValueKind.Object &&
                     totalProp.TryGetProperty("value", out var valueProp))
                total = valueProp.GetInt64();
        }

        var hits = hitsRoot.GetProperty("hits");
        var results = new List<Video>();
        var opts = new JsonSerializerOptions { PropertyNameCaseInsensitive = true };

        foreach (var hit in hits.EnumerateArray())
        {
            if (!hit.TryGetProperty("_source", out var srcEl)) continue;
            var srcJson = srcEl.GetRawText();
            var v = JsonSerializer.Deserialize<Video>(srcJson, opts);
            if (v != null) results.Add(v);
        }

        var totalPages = total == 0 ? 0 : (int)Math.Ceiling(total / (double)pageSize);

        return new SearchPageResult
        {
            Page = page,
            PageSize = pageSize,
            Total = total,
            TotalPages = totalPages,
            Results = results
        };
    }
}

public class SearchPageResult
{
    public int Page { get; set; }
    public int PageSize { get; set; }
    public long Total { get; set; }
    public int TotalPages { get; set; }
    public List<Video> Results { get; set; } = new();
}
