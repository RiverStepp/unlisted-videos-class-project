using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using Microsoft.Extensions.Options;
using YouTubeDataAPI.Models;

namespace YouTubeDataAPI.Services;

public class VideosSearchService
{
    private readonly HttpClient _http;
    private readonly string _index;
    private readonly JsonSerializerOptions _jsonOptions;

    public VideosSearchService(HttpClient http, IOptions<ElasticSettings> settings)
    {
        _http = http;
        _index = settings.Value.Index;
        // Web defaults = camelCase, matches your JSON like "sqlData", "ytMetadata"
        _jsonOptions = new JsonSerializerOptions(JsonSerializerDefaults.Web)
        {
            PropertyNameCaseInsensitive = true
        };
    }

    // Create the index if missing
    public async Task EnsureIndexExistsAsync()
    {
        var head = await _http.SendAsync(new HttpRequestMessage(HttpMethod.Head, _index));
        if (head.StatusCode == HttpStatusCode.NotFound)
        {
            var create = await _http.PutAsync(_index, null); // default settings, dynamic mapping
            if (!create.IsSuccessStatusCode)
            {
                var body = await create.Content.ReadAsStringAsync();
                throw new InvalidOperationException(
                    $"Failed to create index '{_index}'. Status {(int)create.StatusCode}. Body: {body}");
            }
        }
    }

    public async Task IndexVideoAsync(Video v)
    {
        var resp = await _http.PutAsJsonAsync($"{_index}/_doc/{v.Id}", v, _jsonOptions);
        if (!resp.IsSuccessStatusCode)
        {
            var body = await resp.Content.ReadAsStringAsync();
            throw new InvalidOperationException(
                $"Failed to index video {v.Id}. Status {(int)resp.StatusCode}. Body: {body}");
        }
    }

    public async Task<long> CountAsync()
    {
        var resp = await _http.GetAsync($"{_index}/_count");
        if (resp.StatusCode == HttpStatusCode.NotFound)
            return 0;

        resp.EnsureSuccessStatusCode();

        using var stream = await resp.Content.ReadAsStreamAsync();
        using var doc = await JsonDocument.ParseAsync(stream);
        return doc.RootElement.GetProperty("count").GetInt64();
    }

    public async Task<List<Video>> SearchAsync(string query)
    {
        var body = new
        {
            query = new
            {
                multi_match = new
                {
                    query,
                    fields = new[]
                    {
                        "sqlData.videoTitle",
                        "ytMetadata.description",
                        "sqlData.uploader",
                        "sqlData.channel",
                        "ytMetadata.tags"
                    },
                    fuzziness = "AUTO"
                }
            }
        };

        var resp = await _http.PostAsJsonAsync($"{_index}/_search", body, _jsonOptions);
        resp.EnsureSuccessStatusCode();

        using var stream = await resp.Content.ReadAsStreamAsync();
        using var doc = await JsonDocument.ParseAsync(stream);

        var hits = doc.RootElement
            .GetProperty("hits")
            .GetProperty("hits");

        var results = new List<Video>();

        foreach (var hit in hits.EnumerateArray())
        {
            if (!hit.TryGetProperty("_source", out var src)) continue;

            var video = src.Deserialize<Video>(_jsonOptions);
            if (video != null)
                results.Add(video);
        }

        return results;
    }
}
