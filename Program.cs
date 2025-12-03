using MongoDB.Driver;
using Microsoft.Extensions.Options;
using YouTubeDataAPI.Models;
using YouTubeDataAPI.Services;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddControllers();
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

// Mongo config
builder.Services.Configure<MongoDbSettings>(
    builder.Configuration.GetSection("MongoDbSettings"));

builder.Services.AddSingleton<IMongoClient>(sp =>
{
    var cfg = sp.GetRequiredService<IOptions<MongoDbSettings>>().Value;
    return new MongoClient(cfg.ConnectionString);
});

builder.Services.AddScoped(sp =>
{
    var cfg = sp.GetRequiredService<IOptions<MongoDbSettings>>().Value;
    var client = sp.GetRequiredService<IMongoClient>();
    return client.GetDatabase(cfg.DatabaseName);
});

builder.Services.AddScoped<VideosService>();

// Elasticsearch config (HTTP client, no typed ES client)
builder.Services.Configure<ElasticSettings>(
    builder.Configuration.GetSection("Elasticsearch"));

builder.Services.AddHttpClient<VideosSearchService>((sp, client) =>
{
    var cfg = sp.GetRequiredService<IOptions<ElasticSettings>>().Value;
    client.BaseAddress = new Uri(cfg.Url); // e.g. http://localhost:9200
});

var app = builder.Build();

if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}

app.UseHttpsRedirection();
app.UseAuthorization();
app.MapControllers();
app.Run();
