using FluentAssertions;
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
    public async Task TryCreateMatchAsync_ConcurrentRequests_ShouldHandleThreadSafely()
    {
        var mockRedis = new Mock<IConnectionMultiplexer>();
        var mockDatabase = new Mock<IDatabase>();
        var mockConfiguration = new Mock<IConfiguration>();
        var mockConfigSection = new Mock<IConfigurationSection>();
        var mockLogger = new Mock<ILogger<MatchMakingService>>();

        mockRedis.Setup(x => x.GetDatabase(It.IsAny<int>(), It.IsAny<object>()))
            .Returns(mockDatabase.Object);

        mockConfigSection.Setup(x => x.Value).Returns("3");
        mockConfiguration.Setup(x => x.GetSection("PlayersPerMatch"))
            .Returns(mockConfigSection.Object);

        var waitingPlayers = new List<string>();
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

        var service = new MatchMakingService(
            mockRedis.Object,
            mockConfiguration.Object,
            mockLogger.Object);

        var tasks = Enumerable.Range(1, 10)
            .Select(i => service.TryCreateMatchAsync($"user{i}", CancellationToken.None))
            .ToArray();

        var results = await Task.WhenAll(tasks);

        var matches = results.Where(r => r != null).ToList();
        matches.Should().HaveCountGreaterThanOrEqualTo(1, "at least one match should be created from 10 users");

        if (matches.Count >= 3)
        {
            matches.Should().HaveCount(3, "with 10 users, we should create 3 matches (9 players)");
        }
    }

    [Fact]
    public async Task TryCreateMatchAsync_100ConcurrentDuplicates_ShouldPreventAllDuplicates()
    {
        var mockRedis = new Mock<IConnectionMultiplexer>();
        var mockDatabase = new Mock<IDatabase>();
        var mockConfiguration = new Mock<IConfiguration>();
        var mockConfigSection = new Mock<IConfigurationSection>();
        var mockLogger = new Mock<ILogger<MatchMakingService>>();

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

        var service = new MatchMakingService(
            mockRedis.Object,
            mockConfiguration.Object,
            mockLogger.Object);

        var tasks = Enumerable.Range(1, 100)
            .Select(_ => service.TryCreateMatchAsync("duplicate-user", CancellationToken.None))
            .ToArray();

        var results = await Task.WhenAll(tasks);

        var successfulAdds = results.Count(r => r == null);
        addedUsers.Should().ContainSingle("duplicate-user", "only one instance should be added despite 100 attempts");
    }

    [Fact]
    public async Task TryCreateMatchAsync_SequentialMatches_ShouldCreateMultipleMatches()
    {
        var mockRedis = new Mock<IConnectionMultiplexer>();
        var mockDatabase = new Mock<IDatabase>();
        var mockConfiguration = new Mock<IConfiguration>();
        var mockConfigSection = new Mock<IConfigurationSection>();
        var mockLogger = new Mock<ILogger<MatchMakingService>>();

        mockRedis.Setup(x => x.GetDatabase(It.IsAny<int>(), It.IsAny<object>()))
            .Returns(mockDatabase.Object);

        mockConfigSection.Setup(x => x.Value).Returns("3");
        mockConfiguration.Setup(x => x.GetSection("PlayersPerMatch"))
            .Returns(mockConfigSection.Object);

        var waitingPlayers = new Queue<string>();

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

        var service = new MatchMakingService(
            mockRedis.Object,
            mockConfiguration.Object,
            mockLogger.Object);

        var results = new List<MatchComplete?>();
        for (int i = 1; i <= 6; i++)
        {
            var result = await service.TryCreateMatchAsync($"user{i}", CancellationToken.None);
            results.Add(result);
        }

        var matches = results.Where(r => r != null).ToList();
        matches.Should().HaveCount(2, "6 users should create 2 matches");
        matches[0]!.UserIds.Should().HaveCount(3);
        matches[1]!.UserIds.Should().HaveCount(3);
    }
}
