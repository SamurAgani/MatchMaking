using FluentAssertions;
using MatchMaking.Worker.Services.Concretes;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Moq;
using StackExchange.Redis;

namespace MatchMaking.Worker.Test.UnitTests;

public class MatchMakingServiceTests
{
    private readonly Mock<IConnectionMultiplexer> _mockRedis;
    private readonly Mock<IDatabase> _mockDatabase;
    private readonly Mock<IConfiguration> _mockConfiguration;
    private readonly Mock<IConfigurationSection> _mockConfigSection;
    private readonly Mock<ILogger<MatchMakingService>> _mockLogger;
    private readonly MatchMakingService _service;

    public MatchMakingServiceTests()
    {
        _mockRedis = new Mock<IConnectionMultiplexer>();
        _mockDatabase = new Mock<IDatabase>();
        _mockConfiguration = new Mock<IConfiguration>();
        _mockConfigSection = new Mock<IConfigurationSection>();
        _mockLogger = new Mock<ILogger<MatchMakingService>>();

        _mockRedis.Setup(x => x.GetDatabase(It.IsAny<int>(), It.IsAny<object>()))
            .Returns(_mockDatabase.Object);

        _mockConfigSection.Setup(x => x.Value).Returns("3");
        _mockConfiguration.Setup(x => x.GetSection("PlayersPerMatch"))
            .Returns(_mockConfigSection.Object);

        _service = new MatchMakingService(
            _mockRedis.Object,
            _mockConfiguration.Object,
            _mockLogger.Object);
    }

    [Fact]
    public async Task TryCreateMatchAsync_WithLessThan3Players_ReturnsNull()
    {
        var userId = "user1";

        _mockDatabase.Setup(x => x.ListRangeAsync(
            It.Is<RedisKey>(k => k.ToString() == "waiting_players"),
            It.IsAny<long>(),
            It.IsAny<long>(),
            It.IsAny<CommandFlags>()))
            .ReturnsAsync(Array.Empty<RedisValue>());

        _mockDatabase.Setup(x => x.ListRightPushAsync(
            It.IsAny<RedisKey>(),
            It.IsAny<RedisValue>(),
            It.IsAny<When>(),
            It.IsAny<CommandFlags>()))
            .ReturnsAsync(1);

        _mockDatabase.Setup(x => x.ListLengthAsync(
            It.Is<RedisKey>(k => k.ToString() == "waiting_players"),
            It.IsAny<CommandFlags>()))
            .ReturnsAsync(1);

        var result = await _service.TryCreateMatchAsync(userId, CancellationToken.None);

        result.Should().BeNull("not enough players to create a match");
        _mockDatabase.Verify(x => x.ListRightPushAsync(
            It.Is<RedisKey>(k => k.ToString() == "waiting_players"),
            It.Is<RedisValue>(v => v.ToString() == userId),
            It.IsAny<When>(),
            It.IsAny<CommandFlags>()),
            Times.Once);
    }

    [Fact]
    public async Task TryCreateMatchAsync_WithExactly3Players_CreatesMatch()
    {
        var userId = "user3";
        var existingPlayers = new RedisValue[] { "user1", "user2" };

        _mockDatabase.Setup(x => x.ListRangeAsync(
            It.Is<RedisKey>(k => k.ToString() == "waiting_players"),
            It.IsAny<long>(),
            It.IsAny<long>(),
            It.IsAny<CommandFlags>()))
            .ReturnsAsync(existingPlayers);

        _mockDatabase.Setup(x => x.ListRightPushAsync(
            It.IsAny<RedisKey>(),
            It.IsAny<RedisValue>(),
            It.IsAny<When>(),
            It.IsAny<CommandFlags>()))
            .ReturnsAsync(3);

        _mockDatabase.Setup(x => x.ListLengthAsync(
            It.Is<RedisKey>(k => k.ToString() == "waiting_players"),
            It.IsAny<CommandFlags>()))
            .ReturnsAsync(3);

        var playerQueue = new Queue<string>(new[] { "user1", "user2", "user3" });
        _mockDatabase.Setup(x => x.ListLeftPopAsync(
            It.Is<RedisKey>(k => k.ToString() == "waiting_players"),
            It.IsAny<CommandFlags>()))
            .ReturnsAsync(() => playerQueue.Count > 0 ? new RedisValue(playerQueue.Dequeue()) : RedisValue.Null);

        var result = await _service.TryCreateMatchAsync(userId, CancellationToken.None);

        result.Should().NotBeNull("we have enough players");
        result!.UserIds.Should().HaveCount(3);
        result.UserIds.Should().Contain(new[] { "user1", "user2", "user3" });
        result.MatchId.Should().NotBeNullOrEmpty();
    }

    [Fact]
    public async Task TryCreateMatchAsync_WithDuplicateUser_ReturnsNull()
    {
        var userId = "user1";
        var existingPlayers = new RedisValue[] { "user1", "user2" };

        _mockDatabase.Setup(x => x.ListRangeAsync(
            It.Is<RedisKey>(k => k.ToString() == "waiting_players"),
            It.IsAny<long>(),
            It.IsAny<long>(),
            It.IsAny<CommandFlags>()))
            .ReturnsAsync(existingPlayers);

        var result = await _service.TryCreateMatchAsync(userId, CancellationToken.None);

        result.Should().BeNull("user is already in queue");
        _mockDatabase.Verify(x => x.ListRightPushAsync(
            It.IsAny<RedisKey>(),
            It.IsAny<RedisValue>(),
            It.IsAny<When>(),
            It.IsAny<CommandFlags>()),
            Times.Never, "duplicate user should not be added");
    }

    [Fact]
    public async Task TryCreateMatchAsync_WithMoreThan3Players_CreatesMatchAndLeavesRest()
    {
        var userId = "user4";
        var existingPlayers = new RedisValue[] { "user1", "user2", "user3" };

        _mockDatabase.Setup(x => x.ListRangeAsync(
            It.Is<RedisKey>(k => k.ToString() == "waiting_players"),
            It.IsAny<long>(),
            It.IsAny<long>(),
            It.IsAny<CommandFlags>()))
            .ReturnsAsync(existingPlayers);

        _mockDatabase.Setup(x => x.ListRightPushAsync(
            It.IsAny<RedisKey>(),
            It.IsAny<RedisValue>(),
            It.IsAny<When>(),
            It.IsAny<CommandFlags>()))
            .ReturnsAsync(4);

        _mockDatabase.Setup(x => x.ListLengthAsync(
            It.Is<RedisKey>(k => k.ToString() == "waiting_players"),
            It.IsAny<CommandFlags>()))
            .ReturnsAsync(4);

        var playerQueue = new Queue<string>(new[] { "user1", "user2", "user3" });
        _mockDatabase.Setup(x => x.ListLeftPopAsync(
            It.Is<RedisKey>(k => k.ToString() == "waiting_players"),
            It.IsAny<CommandFlags>()))
            .ReturnsAsync(() => playerQueue.Count > 0 ? new RedisValue(playerQueue.Dequeue()) : RedisValue.Null);

        var result = await _service.TryCreateMatchAsync(userId, CancellationToken.None);

        result.Should().NotBeNull("we have enough players");
        result!.UserIds.Should().HaveCount(3, "only 3 players per match");
        _mockDatabase.Verify(x => x.ListLeftPopAsync(
            It.Is<RedisKey>(k => k.ToString() == "waiting_players"),
            It.IsAny<CommandFlags>()),
            Times.Exactly(3), "should pop exactly 3 players");
    }

    [Fact]
    public async Task TryCreateMatchAsync_WhenRedisThrowsException_ThrowsException()
    {
        var userId = "user1";

        _mockDatabase.Setup(x => x.ListRangeAsync(
            It.IsAny<RedisKey>(),
            It.IsAny<long>(),
            It.IsAny<long>(),
            It.IsAny<CommandFlags>()))
            .ThrowsAsync(new RedisConnectionException(ConnectionFailureType.UnableToConnect, "Connection failed"));

        await FluentActions.Invoking(() => _service.TryCreateMatchAsync(userId, CancellationToken.None))
            .Should().ThrowAsync<RedisConnectionException>();
    }
}
