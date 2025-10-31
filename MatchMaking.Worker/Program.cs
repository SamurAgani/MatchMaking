using MatchMaking.Worker;
using MatchMaking.Worker.Services.Abstracts;
using MatchMaking.Worker.Services.Concretes;
using Serilog;
using StackExchange.Redis;

var builder = Host.CreateApplicationBuilder(args);

Log.Logger = new LoggerConfiguration().ReadFrom.Configuration(builder.Configuration).CreateLogger();

builder.Logging.ClearProviders();
builder.Logging.AddSerilog();

builder.Services.AddSingleton<IConnectionMultiplexer>(sp =>
{
    var configuration = sp.GetRequiredService<IConfiguration>();
    var redisConnectionString = configuration["Redis:ConnectionString"] ?? "localhost:6379";
    return ConnectionMultiplexer.Connect(redisConnectionString);
});

builder.Services.AddSingleton<IMatchMakingService, MatchMakingService>();
builder.Services.AddSingleton<IKafkaService, KafkaService>();

builder.Services.AddHostedService<Worker>();

var host = builder.Build();

Log.Information("MatchMaking Worker starting");

host.Run();

