using MongoDB.Driver;
using Microsoft.Extensions.Options;
using YouTubeDataAPI.Models;
using YouTubeDataAPI.Services;
using Elastic.Clients.Elasticsearch;
using Microsoft.Extensions.Options;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddControllers();
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

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

builder.Services.Configure<ElasticSettings>(
    builder.Configuration.GetSection("Elasticsearch"));

builder.Services.AddSingleton(sp =>
{
    var cfg = sp.GetRequiredService<IOptions<ElasticSettings>>().Value;
    return new ElasticsearchClient(new Uri(cfg.Url));
});

builder.Services.AddScoped<VideosSearchService>();

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
