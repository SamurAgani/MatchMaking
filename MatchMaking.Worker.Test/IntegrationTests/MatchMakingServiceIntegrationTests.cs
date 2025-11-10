using Confluent.Kafka;
using FluentAssertions;
using MatchMaking.Infrastructure.Kafka.Abstractions;
using MatchMaking.Shared.Models;
using MatchMaking.Worker.Services.Concretes;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Moq;
using StackExchange.Redis;

namespace MatchMaking.Worker.Test.IntegrationTests;

public class MatchMakingServiceIntegrationTests
{
    [Fact]
    public async Task ProcessMatchRequestAsync_ConcurrentRequests_ShouldHandleThreadSafely()
    {
        var mockRedis = new Mock<IConnectionMultiplexer>();
        var mockDatabase = new Mock<IDatabase>();
        var mockConfiguration = new Mock<IConfiguration>();
        var mockConfigSection = new Mock<IConfigurationSection>();
        var mockLogger = new Mock<ILogger<MatchMakingService>>();
        var mockProducer = new Mock<IKafkaProducer<string, MatchComplete>>();

        mockRedis.Setup(x => x.GetDatabase(It.IsAny<int>(), It.IsAny<object>()))
            .Returns(mockDatabase.Object);

        mockConfigSection.Setup(x => x.Value).Returns("3");
        mockConfiguration.Setup(x => x.GetSection("PlayersPerMatch"))
            .Returns(mockConfigSection.Object);

        var waitingPlayers = new List<string>();
        var lockObj = new object();
        var matches = new List<MatchComplete>();

        mockDatabase.Setup(x => x.ListRangeAsync(
            It.Is<RedisKey>(k => k.ToString() == "waiting_players"),
            It.IsAny<long>(),
            It.IsAny<long>(),
            It.IsAny<CommandFlags>()))
            .ReturnsAsync(() =>
            {
                lock (lockObj)
                {
                    return waitingPlayers.Select(p => (RedisValue)p).ToArray();
                }
            });

        mockDatabase.Setup(x => x.ListRightPushAsync(
            It.Is<RedisKey>(k => k.ToString() == "waiting_players"),
            It.IsAny<RedisValue>(),
            It.IsAny<When>(),
            It.IsAny<CommandFlags>()))
            .ReturnsAsync((RedisKey k, RedisValue v, When w, CommandFlags f) =>
            {
                lock (lockObj)
                {
                    waitingPlayers.Add(v.ToString());
                    return waitingPlayers.Count;
                }
            });

        mockDatabase.Setup(x => x.ListLengthAsync(
            It.Is<RedisKey>(k => k.ToString() == "waiting_players"),
            It.IsAny<CommandFlags>()))
            .ReturnsAsync(() =>
            {
                lock (lockObj)
                {
                    return waitingPlayers.Count;
                }
            });

        mockDatabase.Setup(x => x.ListLeftPopAsync(
            It.Is<RedisKey>(k => k.ToString() == "waiting_players"),
            It.IsAny<CommandFlags>()))
            .ReturnsAsync(() =>
            {
                lock (lockObj)
                {
                    if (waitingPlayers.Count > 0)
                    {
                        var player = waitingPlayers[0];
                        waitingPlayers.RemoveAt(0);
                        return new RedisValue(player);
                    }
                    return RedisValue.Null;
                }
            });

        mockProducer.Setup(x => x.ProduceAsync(
            It.IsAny<string>(),
            It.IsAny<string>(),
            It.IsAny<MatchComplete>(),
            It.IsAny<CancellationToken>()))
            .ReturnsAsync((string topic, string key, MatchComplete value, CancellationToken ct) =>
            {
                lock (lockObj)
                {
                    matches.Add(value);
                }
                return new DeliveryResult<string, MatchComplete>
                {
                    Status = PersistenceStatus.Persisted,
                    Offset = new Offset(0)
                };
            });

        var service = new MatchMakingService(
            mockRedis.Object,
            mockConfiguration.Object,
            mockLogger.Object,
            mockProducer.Object);

        var tasks = Enumerable.Range(1, 10)
            .Select(i =>
            {
                var matchRequest = new MatchRequest($"user{i}");
                var message = new Message<string, MatchRequest>
                {
                    Key = $"user{i}",
                    Value = matchRequest
                };
                var consumeResult = new ConsumeResult<string, MatchRequest>
                {
                    Message = message
                };
                return service.ProcessMatchRequestAsync(consumeResult, CancellationToken.None);
            })
            .ToArray();

        var results = await Task.WhenAll(tasks);

        results.Should().AllBeEquivalentTo(true, "all requests should be processed successfully");
        matches.Should().HaveCountGreaterThanOrEqualTo(1, "at least one match should be created from 10 users");

        if (matches.Count >= 3)
        {
            matches.Should().HaveCount(3, "with 10 users, we should create 3 matches (9 players)");
        }
    }

    [Fact]
    public async Task ProcessMatchRequestAsync_100ConcurrentDuplicates_ShouldPreventAllDuplicates()
    {
        var mockRedis = new Mock<IConnectionMultiplexer>();
        var mockDatabase = new Mock<IDatabase>();
        var mockConfiguration = new Mock<IConfiguration>();
        var mockConfigSection = new Mock<IConfigurationSection>();
        var mockLogger = new Mock<ILogger<MatchMakingService>>();
        var mockProducer = new Mock<IKafkaProducer<string, MatchComplete>>();

        mockRedis.Setup(x => x.GetDatabase(It.IsAny<int>(), It.IsAny<object>()))
            .Returns(mockDatabase.Object);

        mockConfigSection.Setup(x => x.Value).Returns("3");
        mockConfiguration.Setup(x => x.GetSection("PlayersPerMatch"))
            .Returns(mockConfigSection.Object);

        var addedUsers = new HashSet<string>();
        var lockObj = new object();

        mockDatabase.Setup(x => x.ListRangeAsync(
            It.Is<RedisKey>(k => k.ToString() == "waiting_players"),
            It.IsAny<long>(),
            It.IsAny<long>(),
            It.IsAny<CommandFlags>()))
            .ReturnsAsync(() =>
            {
                lock (lockObj)
                {
                    return addedUsers.Select(u => (RedisValue)u).ToArray();
                }
            });

        mockDatabase.Setup(x => x.ListRightPushAsync(
            It.Is<RedisKey>(k => k.ToString() == "waiting_players"),
            It.IsAny<RedisValue>(),
            It.IsAny<When>(),
            It.IsAny<CommandFlags>()))
            .ReturnsAsync((RedisKey k, RedisValue v, When w, CommandFlags f) =>
            {
                lock (lockObj)
                {
                    addedUsers.Add(v.ToString());
                    return addedUsers.Count;
                }
            });

        mockDatabase.Setup(x => x.ListLengthAsync(
            It.Is<RedisKey>(k => k.ToString() == "waiting_players"),
            It.IsAny<CommandFlags>()))
            .ReturnsAsync(() =>
            {
                lock (lockObj)
                {
                    return addedUsers.Count;
                }
            });

        mockProducer.Setup(x => x.ProduceAsync(
            It.IsAny<string>(),
            It.IsAny<string>(),
            It.IsAny<MatchComplete>(),
            It.IsAny<CancellationToken>()))
            .ReturnsAsync(new DeliveryResult<string, MatchComplete>
            {
                Status = PersistenceStatus.Persisted,
                Offset = new Offset(0)
            });

        var service = new MatchMakingService(
            mockRedis.Object,
            mockConfiguration.Object,
            mockLogger.Object,
            mockProducer.Object);

        var tasks = Enumerable.Range(1, 100)
            .Select(_ =>
            {
                var matchRequest = new MatchRequest("duplicate-user");
                var message = new Message<string, MatchRequest>
                {
                    Key = "duplicate-user",
                    Value = matchRequest
                };
                var consumeResult = new ConsumeResult<string, MatchRequest>
                {
                    Message = message
                };
                return service.ProcessMatchRequestAsync(consumeResult, CancellationToken.None);
            })
            .ToArray();

        var results = await Task.WhenAll(tasks);

        results.Should().AllBeEquivalentTo(true, "all requests should be processed successfully");
        addedUsers.Should().ContainSingle("duplicate-user", "only one instance should be added despite 100 attempts");
    }

    [Fact]
    public async Task ProcessMatchRequestAsync_SequentialMatches_ShouldCreateMultipleMatches()
    {
        var mockRedis = new Mock<IConnectionMultiplexer>();
        var mockDatabase = new Mock<IDatabase>();
        var mockConfiguration = new Mock<IConfiguration>();
        var mockConfigSection = new Mock<IConfigurationSection>();
        var mockLogger = new Mock<ILogger<MatchMakingService>>();
        var mockProducer = new Mock<IKafkaProducer<string, MatchComplete>>();

        mockRedis.Setup(x => x.GetDatabase(It.IsAny<int>(), It.IsAny<object>()))
            .Returns(mockDatabase.Object);

        mockConfigSection.Setup(x => x.Value).Returns("3");
        mockConfiguration.Setup(x => x.GetSection("PlayersPerMatch"))
            .Returns(mockConfigSection.Object);

        var waitingPlayers = new Queue<string>();
        var matches = new List<MatchComplete>();

        mockDatabase.Setup(x => x.ListRangeAsync(
            It.Is<RedisKey>(k => k.ToString() == "waiting_players"),
            It.IsAny<long>(),
            It.IsAny<long>(),
            It.IsAny<CommandFlags>()))
            .ReturnsAsync(() => waitingPlayers.Select(p => (RedisValue)p).ToArray());

        mockDatabase.Setup(x => x.ListRightPushAsync(
            It.Is<RedisKey>(k => k.ToString() == "waiting_players"),
            It.IsAny<RedisValue>(),
            It.IsAny<When>(),
            It.IsAny<CommandFlags>()))
            .ReturnsAsync((RedisKey k, RedisValue v, When w, CommandFlags f) =>
            {
                waitingPlayers.Enqueue(v.ToString());
                return waitingPlayers.Count;
            });

        mockDatabase.Setup(x => x.ListLengthAsync(
            It.Is<RedisKey>(k => k.ToString() == "waiting_players"),
            It.IsAny<CommandFlags>()))
            .ReturnsAsync(() => waitingPlayers.Count);

        mockDatabase.Setup(x => x.ListLeftPopAsync(
            It.Is<RedisKey>(k => k.ToString() == "waiting_players"),
            It.IsAny<CommandFlags>()))
            .ReturnsAsync(() => waitingPlayers.Count > 0 ? new RedisValue(waitingPlayers.Dequeue()) : RedisValue.Null);

        mockProducer.Setup(x => x.ProduceAsync(
            It.IsAny<string>(),
            It.IsAny<string>(),
            It.IsAny<MatchComplete>(),
            It.IsAny<CancellationToken>()))
            .ReturnsAsync((string topic, string key, MatchComplete value, CancellationToken ct) =>
            {
                matches.Add(value);
                return new DeliveryResult<string, MatchComplete>
                {
                    Status = PersistenceStatus.Persisted,
                    Offset = new Offset(0)
                };
            });

        var service = new MatchMakingService(
            mockRedis.Object,
            mockConfiguration.Object,
            mockLogger.Object,
            mockProducer.Object);

        var results = new List<bool>();
        for (int i = 1; i <= 6; i++)
        {
            var matchRequest = new MatchRequest($"user{i}");
            var message = new Message<string, MatchRequest>
            {
                Key = $"user{i}",
                Value = matchRequest
            };
            var consumeResult = new ConsumeResult<string, MatchRequest>
            {
                Message = message
            };
            var result = await service.ProcessMatchRequestAsync(consumeResult, CancellationToken.None);
            results.Add(result);
        }

        results.Should().AllBeEquivalentTo(true, "all requests should be processed successfully");
        matches.Should().HaveCount(2, "6 users should create 2 matches");
        matches[0].UserIds.Should().HaveCount(3);
        matches[1].UserIds.Should().HaveCount(3);
    }
}
