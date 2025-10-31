using MatchMaking.Service.BackgroundServices;
using MatchMaking.Service.Middlewares;
using MatchMaking.Service.Services.Abstracts;
using MatchMaking.Service.Services.Concrete;
using Serilog;
using StackExchange.Redis;


var builder = WebApplication.CreateBuilder(args);

builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

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

builder.Services.AddSingleton<IMatchmakingService, MatchmakingService>();
builder.Services.AddSingleton<IRateLimitService, RedisRateLimitService>();
builder.Services.AddSingleton<IMatchStorageService, RedisMatchStorageService>();

builder.Services.AddSingleton<IKafkaService, KafkaService>();
builder.Services.AddHostedService<KafkaBackgroundService>();

var app = builder.Build();

if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}

app.UseHttpsRedirection();

app.UseMiddleware<ExceptionHandlingMiddleware>();
app.UseMiddleware<RateLimitMiddleware>();

app.UseAuthorization();

app.MapControllers();

Log.Information("MatchMaking Service started successfully");

app.Run();