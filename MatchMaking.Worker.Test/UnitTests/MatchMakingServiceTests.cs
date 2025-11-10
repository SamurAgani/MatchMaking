using Confluent.Kafka;
using FluentAssertions;
using MatchMaking.Infrastructure.Kafka.Abstractions;
using MatchMaking.Shared.Models;
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
    private readonly Mock<IKafkaProducer<string, MatchComplete>> _mockProducer;
    private readonly MatchMakingService _service;

    public MatchMakingServiceTests()
    {
        _mockRedis = new Mock<IConnectionMultiplexer>();
        _mockDatabase = new Mock<IDatabase>();
        _mockConfiguration = new Mock<IConfiguration>();
        _mockConfigSection = new Mock<IConfigurationSection>();
        _mockLogger = new Mock<ILogger<MatchMakingService>>();
        _mockProducer = new Mock<IKafkaProducer<string, MatchComplete>>();

        _mockRedis.Setup(x => x.GetDatabase(It.IsAny<int>(), It.IsAny<object>()))
            .Returns(_mockDatabase.Object);

        _mockConfigSection.Setup(x => x.Value).Returns("3");
        _mockConfiguration.Setup(x => x.GetSection("PlayersPerMatch"))
            .Returns(_mockConfigSection.Object);

        _mockProducer.Setup(x => x.ProduceAsync(
            It.IsAny<string>(),
            It.IsAny<string>(),
            It.IsAny<MatchComplete>(),
            It.IsAny<CancellationToken>()))
            .ReturnsAsync(new DeliveryResult<string, MatchComplete>
            {
                Status = PersistenceStatus.Persisted,
                Offset = new Offset(0)
            });

        _service = new MatchMakingService(
            _mockRedis.Object,
            _mockConfiguration.Object,
            _mockLogger.Object,
            _mockProducer.Object);
    }

    [Fact]
    public async Task ProcessMatchRequestAsync_WithLessThan3Players_ReturnsTrue()
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

        var matchRequest = new MatchRequest(userId);
        var message = new Message<string, MatchRequest>
        {
            Key = userId,
            Value = matchRequest
        };
        var consumeResult = new ConsumeResult<string, MatchRequest>
        {
            Message = message
        };

        var result = await _service.ProcessMatchRequestAsync(consumeResult, CancellationToken.None);

        result.Should().BeTrue("request should be processed successfully");
        _mockDatabase.Verify(x => x.ListRightPushAsync(
            It.Is<RedisKey>(k => k.ToString() == "waiting_players"),
            It.Is<RedisValue>(v => v.ToString() == userId),
            It.IsAny<When>(),
            It.IsAny<CommandFlags>()),
            Times.Once);
        _mockProducer.Verify(x => x.ProduceAsync(
            It.IsAny<string>(),
            It.IsAny<string>(),
            It.IsAny<MatchComplete>(),
            It.IsAny<CancellationToken>()),
            Times.Never, "no match should be created with less than 3 players");
    }

    [Fact]
    public async Task ProcessMatchRequestAsync_WithExactly3Players_CreatesMatch()
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

        var matchRequest = new MatchRequest(userId);
        var message = new Message<string, MatchRequest>
        {
            Key = userId,
            Value = matchRequest
        };
        var consumeResult = new ConsumeResult<string, MatchRequest>
        {
            Message = message
        };

        var result = await _service.ProcessMatchRequestAsync(consumeResult, CancellationToken.None);

        result.Should().BeTrue("request should be processed successfully");
        _mockProducer.Verify(x => x.ProduceAsync(
            It.IsAny<string>(),
            It.IsAny<string>(),
            It.Is<MatchComplete>(m => m.UserIds.Count == 3 &&
                                      m.UserIds.Contains("user1") &&
                                      m.UserIds.Contains("user2") &&
                                      m.UserIds.Contains("user3")),
            It.IsAny<CancellationToken>()),
            Times.Once, "a match should be created and published");
    }

    [Fact]
    public async Task ProcessMatchRequestAsync_WithDuplicateUser_ReturnsTrue()
    {
        var userId = "user1";
        var existingPlayers = new RedisValue[] { "user1", "user2" };

        _mockDatabase.Setup(x => x.ListRangeAsync(
            It.Is<RedisKey>(k => k.ToString() == "waiting_players"),
            It.IsAny<long>(),
            It.IsAny<long>(),
            It.IsAny<CommandFlags>()))
            .ReturnsAsync(existingPlayers);

        var matchRequest = new MatchRequest(userId);
        var message = new Message<string, MatchRequest>
        {
            Key = userId,
            Value = matchRequest
        };
        var consumeResult = new ConsumeResult<string, MatchRequest>
        {
            Message = message
        };

        var result = await _service.ProcessMatchRequestAsync(consumeResult, CancellationToken.None);

        result.Should().BeTrue("request should be processed successfully even if user is duplicate");
        _mockDatabase.Verify(x => x.ListRightPushAsync(
            It.IsAny<RedisKey>(),
            It.IsAny<RedisValue>(),
            It.IsAny<When>(),
            It.IsAny<CommandFlags>()),
            Times.Never, "duplicate user should not be added");
        _mockProducer.Verify(x => x.ProduceAsync(
            It.IsAny<string>(),
            It.IsAny<string>(),
            It.IsAny<MatchComplete>(),
            It.IsAny<CancellationToken>()),
            Times.Never, "no match should be created for duplicate user");
    }

    [Fact]
    public async Task ProcessMatchRequestAsync_WithMoreThan3Players_CreatesMatchAndLeavesRest()
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

        var matchRequest = new MatchRequest(userId);
        var message = new Message<string, MatchRequest>
        {
            Key = userId,
            Value = matchRequest
        };
        var consumeResult = new ConsumeResult<string, MatchRequest>
        {
            Message = message
        };

        var result = await _service.ProcessMatchRequestAsync(consumeResult, CancellationToken.None);

        result.Should().BeTrue("request should be processed successfully");
        _mockProducer.Verify(x => x.ProduceAsync(
            It.IsAny<string>(),
            It.IsAny<string>(),
            It.Is<MatchComplete>(m => m.UserIds.Count == 3),
            It.IsAny<CancellationToken>()),
            Times.Once, "a match should be created with exactly 3 players");
        _mockDatabase.Verify(x => x.ListLeftPopAsync(
            It.Is<RedisKey>(k => k.ToString() == "waiting_players"),
            It.IsAny<CommandFlags>()),
            Times.Exactly(3), "should pop exactly 3 players");
    }

    [Fact]
    public async Task ProcessMatchRequestAsync_WhenRedisThrowsException_ReturnsFalse()
    {
        var userId = "user1";

        _mockDatabase.Setup(x => x.ListRangeAsync(
            It.IsAny<RedisKey>(),
            It.IsAny<long>(),
            It.IsAny<long>(),
            It.IsAny<CommandFlags>()))
            .ThrowsAsync(new RedisConnectionException(ConnectionFailureType.UnableToConnect, "Connection failed"));

        var matchRequest = new MatchRequest(userId);
        var message = new Message<string, MatchRequest>
        {
            Key = userId,
            Value = matchRequest
        };
        var consumeResult = new ConsumeResult<string, MatchRequest>
        {
            Message = message
        };

        var result = await _service.ProcessMatchRequestAsync(consumeResult, CancellationToken.None);

        result.Should().BeFalse("exception should be caught and false returned");
    }
}
