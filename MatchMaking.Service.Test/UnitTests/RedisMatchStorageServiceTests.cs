using FluentAssertions;
using MatchMaking.Service.Services.Concrete;
using MatchMaking.Shared.Models;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Moq;
using StackExchange.Redis;

namespace MatchMaking.Service.Test.UnitTests;

public class RedisMatchStorageServiceTests
{
    private readonly Mock<IConnectionMultiplexer> _mockRedis;
    private readonly Mock<IDatabase> _mockDatabase;
    private readonly Mock<ILogger<RedisMatchStorageService>> _mockLogger;
    private readonly RedisMatchStorageService _service;

    public RedisMatchStorageServiceTests()
    {
        _mockRedis = new Mock<IConnectionMultiplexer>();
        _mockDatabase = new Mock<IDatabase>();
        _mockLogger = new Mock<ILogger<RedisMatchStorageService>>();

        _mockRedis.Setup(x => x.GetDatabase(It.IsAny<int>(), It.IsAny<object>()))
            .Returns(_mockDatabase.Object);

        _service = new RedisMatchStorageService(_mockRedis.Object, _mockLogger.Object);
    }

    [Fact]
    public async Task StoreMatchForUserAsync_StoresMatchInRedis()
    {
        var userId = "user123";
        var matchId = "match-id-123";
        var userIds = new List<string> { "user1", "user2", "user3" };
        var match = new MatchComplete(matchId, userIds);

        _mockDatabase.Setup(x => x.StringSetAsync(
            It.IsAny<RedisKey>(),
            It.IsAny<RedisValue>(),
            It.IsAny<TimeSpan?>(),
            It.IsAny<bool>(),
            It.IsAny<When>(),
            It.IsAny<CommandFlags>()))
            .ReturnsAsync(true);

        await _service.StoreMatchForUserAsync(userId, match, CancellationToken.None);

        _mockDatabase.Verify(x => x.StringSetAsync(
            It.Is<RedisKey>(k => k.ToString() == $"user:{userId}:match"),
            It.IsAny<RedisValue>(),
            It.IsAny<TimeSpan?>(),
            It.IsAny<bool>(),
            It.IsAny<When>(),
            It.IsAny<CommandFlags>()),
            Times.Once);
    }

    [Fact]
    public async Task GetMatchForUserAsync_WhenMatchExists_ReturnsMatch()
    {
        var userId = "user123";
        var matchId = "match-id-123";
        var matchJson = $@"{{""MatchId"":""{matchId}"",""UserIds"":[""user1"",""user2"",""user3""]}}";

        _mockDatabase.Setup(x => x.StringGetAsync(
            It.Is<RedisKey>(k => k.ToString() == $"user:{userId}:match"),
            It.IsAny<CommandFlags>()))
            .ReturnsAsync(new RedisValue(matchJson));

        var result = await _service.GetMatchForUserAsync(userId, CancellationToken.None);

        result.Value.Should().NotBeNull();
        result.Value!.MatchId.Should().Be(matchId);
        result.Value.UserIds.Should().HaveCount(3);
    }

    [Fact]
    public async Task GetMatchForUserAsync_WhenNoMatch_ReturnsNull()
    {
        var userId = "user123";

        _mockDatabase.Setup(x => x.StringGetAsync(
            It.Is<RedisKey>(k => k.ToString() == $"user:{userId}:match"),
            It.IsAny<CommandFlags>()))
            .ReturnsAsync(RedisValue.Null);

        var result = await _service.GetMatchForUserAsync(userId, CancellationToken.None);

        result.Value.Should().BeNull();
    }
}
