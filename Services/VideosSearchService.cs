using Elastic.Clients.Elasticsearch;
using Microsoft.Extensions.Options;
using YouTubeDataAPI.Models;

namespace YouTubeDataAPI.Services;

public class VideosSearchService
{
    private readonly ElasticsearchClient _es;
    private readonly string _index;

    public VideosSearchService(ElasticsearchClient es, IOptions<ElasticSettings> settings)
    {
        _es = es;
        _index = settings.Value.Index;
    }

    public Task IndexVideoAsync(Video v) =>
        _es.IndexAsync(v, idx => idx.Index(_index).Id(v.Id));

    public async Task<List<Video>> SearchAsync(string query)
    {
        var resp = await _es.SearchAsync<Video>(s => s
            .Index(_index)
            .Query(q => q
                .MultiMatch(mm => mm
                    .Query(query)
                    .Fields(new[]
                    {
                        "sqlData.videoTitle",
                        "ytMetadata.description",
                        "sqlData.uploader",
                        "sqlData.channel",
                        "ytMetadata.tags"
                    })
                    .Fuzziness("AUTO")
                )
            )
        );

        return resp.Documents.ToList();
    }

    public async Task<long> CountAsync()
    {
        // No index argument – your client overload here is CountAsync<T>(CancellationToken)
        var countResp = await _es.CountAsync<Video>();
        Console.WriteLine($"ES doc count: {countResp.Count}");
        return countResp.Count;
    }
}
