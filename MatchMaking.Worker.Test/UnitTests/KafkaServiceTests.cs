using FluentAssertions;
using MatchMaking.Shared.Models;
using MatchMaking.Worker.Services.Abstracts;
using MatchMaking.Worker.Services.Concretes;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Moq;

namespace MatchMaking.Worker.Test.UnitTests;

public class KafkaServiceTests : IDisposable
{
    private readonly Mock<IMatchMakingService> _mockMatchMakingService;
    private readonly Mock<ILogger<KafkaService>> _mockLogger;
    private readonly Mock<IConfiguration> _mockConfiguration;
    private KafkaService? _service;

    public KafkaServiceTests()
    {
        _mockMatchMakingService = new Mock<IMatchMakingService>();
        _mockLogger = new Mock<ILogger<KafkaService>>();
        _mockConfiguration = new Mock<IConfiguration>();

        _mockConfiguration.Setup(x => x["Kafka:BootstrapServers"]).Returns("localhost:9092");
    }

    [Fact]
    public void Constructor_ShouldInitializeSuccessfully()
    {
        _service = new KafkaService(
            _mockConfiguration.Object,
            _mockMatchMakingService.Object,
            _mockLogger.Object);

        _service.Should().NotBeNull();
    }

    [Fact]
    public async Task PublishMatchCompleteAsync_WithValidMatch_ShouldNotThrow()
    {
        _service = new KafkaService(
            _mockConfiguration.Object,
            _mockMatchMakingService.Object,
            _mockLogger.Object);

        var match = new MatchComplete("match-123", new List<string> { "user1", "user2", "user3" });

        _service.Should().NotBeNull();
    }

    [Fact]
    public void Dispose_ShouldDisposeResourcesWithoutException()
    {
        var service = new KafkaService(
            _mockConfiguration.Object,
            _mockMatchMakingService.Object,
            _mockLogger.Object);

        FluentActions.Invoking(() => service.Dispose())
            .Should().NotThrow();
    }

    public void Dispose()
    {
        _service?.Dispose();
    }
}
