using MatchMaking.Infrastructure.Kafka.Extensions;
using MatchMaking.Service.BackgroundServices;
using MatchMaking.Service.Middlewares;
using MatchMaking.Service.Services.Abstracts;
using MatchMaking.Service.Services.Concrete;
using MatchMaking.Shared.Models;
using Serilog;
using StackExchange.Redis;


var builder = WebApplication.CreateBuilder(args);

builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();
builder.Services.AddHealthChecks();

Log.Logger = new LoggerConfiguration().ReadFrom.Configuration(builder.Configuration).CreateLogger();

builder.Host.UseSerilog();

builder.Services.AddControllers();
builder.Services.AddOpenApi();

builder.Services.AddSingleton<IConnectionMultiplexer>(sp =>
{
    var configuration = sp.GetRequiredService<IConfiguration>();
    var redisConnectionString = configuration["Redis:ConnectionString"] ?? "localhost:6379";
    return ConnectionMultiplexer.Connect(redisConnectionString);
});

builder.Services.AddSingleton(TimeProvider.System);
builder.Services.AddSingleton<IRateLimitService, RedisRateLimitService>();
builder.Services.AddSingleton<IMatchStorageService, RedisMatchStorageService>();

builder.Services.AddSingleton<IMatchProcessingService, MatchProcessingService>();

builder.Services.AddKafkaProducer<string, MatchRequest>(builder.Configuration, "Kafka:Producer:Service");
builder.Services.AddKafkaConsumer<string, MatchComplete>(builder.Configuration, "Kafka:Consumer:Service");

builder.Services.AddHostedService<KafkaBackgroundService>();
builder.Services.AddSingleton<IMatchmakingService, MatchmakingService>();

var app = builder.Build();

if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}

app.UseHttpsRedirection();

app.UseMiddleware<ExceptionHandlingMiddleware>();
app.UseMiddleware<RateLimitMiddleware>();

app.UseHealthChecks("/health");

app.UseAuthorization();

app.MapControllers();

Log.Information("MatchMaking Service started successfully");

app.Run();