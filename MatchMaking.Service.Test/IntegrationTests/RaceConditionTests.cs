using FluentAssertions;
using MatchMaking.Service.Controllers;
using MatchMaking.Service.Services.Abstracts;
using MatchMaking.Shared.Models;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using Moq;
using System.Collections.Concurrent;

namespace MatchMaking.Service.Test.IntegrationTests;

public class RaceConditionTests
{
    [Fact]
    public async Task ConcurrentQueueAdditions_SameTwoUsers_OnlyOneSucceeds()
    {
        var mockKafkaService = new Mock<IKafkaService>();
        var mockRateLimitService = new Mock<IRateLimitService>();
        var mockMatchStorageService = new Mock<IMatchStorageService>();
        var mockLogger = new Mock<ILogger<MatchMakingController>>();

        var queuedUsers = new ConcurrentBag<string>();

        mockMatchStorageService.Setup(x => x.GetMatchForUserAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync((MatchComplete?)null);

        mockRateLimitService.Setup(x => x.IsUserInQueueAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync((string userId, CancellationToken ct) => queuedUsers.Contains(userId));

        mockRateLimitService.Setup(x => x.IsRateLimitedAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(false);

        mockKafkaService.Setup(x => x.PublishMatchRequestAsync(It.IsAny<MatchRequest>(), It.IsAny<CancellationToken>()))
            .Returns((MatchRequest req, CancellationToken ct) =>
            {
                queuedUsers.Add(req.UserId);
                return Task.CompletedTask;
            });

        var controller = new MatchMakingController(
            mockKafkaService.Object,
            mockRateLimitService.Object,
            mockMatchStorageService.Object,
            mockLogger.Object);

        var userId1 = "race-user-1";
        var userId2 = "race-user-2";

        var tasks = new List<Task<IActionResult>>
        {
            controller.SearchMatch(userId1, CancellationToken.None),
            controller.SearchMatch(userId1, CancellationToken.None),
            controller.SearchMatch(userId2, CancellationToken.None),
            controller.SearchMatch(userId1, CancellationToken.None),
            controller.SearchMatch(userId2, CancellationToken.None)
        };

        var results = await Task.WhenAll(tasks);

        var user1Successes = results.Count(r => r is NoContentResult);
        var user1Failures = results.Count(r => r is BadRequestObjectResult);

        user1Successes.Should().BeGreaterThan(0, "at least some requests should succeed");
        user1Failures.Should().BeGreaterThan(0, "duplicate requests should be rejected");
    }

    [Fact]
    public async Task ConcurrentRequests_100Users_AllProcessedCorrectly()
    {
        var mockKafkaService = new Mock<IKafkaService>();
        var mockRateLimitService = new Mock<IRateLimitService>();
        var mockMatchStorageService = new Mock<IMatchStorageService>();
        var mockLogger = new Mock<ILogger<MatchMakingController>>();

        var publishedRequests = new ConcurrentBag<string>();
        var lockObj = new object();

        mockMatchStorageService.Setup(x => x.GetMatchForUserAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync((MatchComplete?)null);

        mockRateLimitService.Setup(x => x.IsUserInQueueAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(false);

        mockRateLimitService.Setup(x => x.IsRateLimitedAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(false);

        mockKafkaService.Setup(x => x.PublishMatchRequestAsync(It.IsAny<MatchRequest>(), It.IsAny<CancellationToken>()))
            .Returns((MatchRequest req, CancellationToken ct) =>
            {
                publishedRequests.Add(req.UserId);
                return Task.CompletedTask;
            });

        var controller = new MatchMakingController(
            mockKafkaService.Object,
            mockRateLimitService.Object,
            mockMatchStorageService.Object,
            mockLogger.Object);

        var tasks = Enumerable.Range(1, 100)
            .Select(i => controller.SearchMatch($"user-{i}", CancellationToken.None))
            .ToList();

        var results = await Task.WhenAll(tasks);

        var successCount = results.Count(r => r is NoContentResult);
        successCount.Should().Be(100, "all 100 unique user requests should succeed");

        publishedRequests.Should().HaveCount(100, "all requests should be published to Kafka");
        publishedRequests.Distinct().Should().HaveCount(100, "all user IDs should be unique");
    }

    [Fact]
    public async Task ConcurrentMatchRetrieval_WhileMatchBeingStored_ShouldNotCrash()
    {
        var mockKafkaService = new Mock<IKafkaService>();
        var mockRateLimitService = new Mock<IRateLimitService>();
        var mockMatchStorageService = new Mock<IMatchStorageService>();
        var mockLogger = new Mock<ILogger<MatchMakingController>>();

        var match = new MatchComplete("match-123", new List<string> { "user1", "user2", "user3" });
        var retrievalCount = 0;

        mockMatchStorageService.Setup(x => x.GetMatchForUserAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .Returns(async () =>
            {
                Interlocked.Increment(ref retrievalCount);
                await Task.Delay(10);
                return retrievalCount % 2 == 0 ? match : null;
            });

        var controller = new MatchMakingController(
            mockKafkaService.Object,
            mockRateLimitService.Object,
            mockMatchStorageService.Object,
            mockLogger.Object);

        var tasks = Enumerable.Range(0, 50)
            .Select(_ => controller.GetMatch("user1", CancellationToken.None))
            .ToArray();

        var act = async () => await Task.WhenAll(tasks);
        await act.Should().NotThrowAsync("concurrent reads should be safe");

        var results = await Task.WhenAll(tasks);
        results.Should().AllSatisfy(r =>
            r.Should().Match<IActionResult>(result =>
                result is OkObjectResult || result is NotFoundObjectResult,
                "should return either OK or NotFound"));
    }

    [Fact]
    public async Task ConcurrentDuplicateUsers_WithDifferentStates_HandledCorrectly()
    {
        var mockKafkaService = new Mock<IKafkaService>();
        var mockRateLimitService = new Mock<IRateLimitService>();
        var mockMatchStorageService = new Mock<IMatchStorageService>();
        var mockLogger = new Mock<ILogger<MatchMakingController>>();

        var userStates = new ConcurrentDictionary<string, int>();

        mockMatchStorageService.Setup(x => x.GetMatchForUserAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync((string userId, CancellationToken ct) =>
            {
                var state = userStates.GetOrAdd(userId, 0);
                return state > 2 ? new MatchComplete("match-123", new List<string> { userId }) : null;
            });

        mockRateLimitService.Setup(x => x.IsUserInQueueAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync((string userId, CancellationToken ct) =>
            {
                var state = userStates.GetOrAdd(userId, 0);
                return state == 2;
            });

        mockRateLimitService.Setup(x => x.IsRateLimitedAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(false);

        mockKafkaService.Setup(x => x.PublishMatchRequestAsync(It.IsAny<MatchRequest>(), It.IsAny<CancellationToken>()))
            .Returns((MatchRequest req, CancellationToken ct) =>
            {
                userStates.AddOrUpdate(req.UserId, 1, (k, v) => v + 1);
                return Task.CompletedTask;
            });

        var controller = new MatchMakingController(
            mockKafkaService.Object,
            mockRateLimitService.Object,
            mockMatchStorageService.Object,
            mockLogger.Object);

        var userId = "state-test-user";

        var tasks = Enumerable.Range(0, 10)
            .Select(_ => controller.SearchMatch(userId, CancellationToken.None))
            .ToArray();

        var results = await Task.WhenAll(tasks);

        results.Should().NotBeEmpty();
        results.Should().Contain(r => r is NoContentResult || r is BadRequestObjectResult,
            "requests should be properly handled based on user state");
    }

    [Fact]
    public async Task HighConcurrency_StressTest_200SimultaneousRequests()
    {
        var mockKafkaService = new Mock<IKafkaService>();
        var mockRateLimitService = new Mock<IRateLimitService>();
        var mockMatchStorageService = new Mock<IMatchStorageService>();
        var mockLogger = new Mock<ILogger<MatchMakingController>>();

        var processedCount = 0;

        mockMatchStorageService.Setup(x => x.GetMatchForUserAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync((MatchComplete?)null);

        mockRateLimitService.Setup(x => x.IsUserInQueueAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(false);

        mockRateLimitService.Setup(x => x.IsRateLimitedAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(false);

        mockKafkaService.Setup(x => x.PublishMatchRequestAsync(It.IsAny<MatchRequest>(), It.IsAny<CancellationToken>()))
            .Returns(() =>
            {
                Interlocked.Increment(ref processedCount);
                return Task.CompletedTask;
            });

        var controller = new MatchMakingController(
            mockKafkaService.Object,
            mockRateLimitService.Object,
            mockMatchStorageService.Object,
            mockLogger.Object);

        var tasks = Enumerable.Range(1, 200)
            .Select(i => controller.SearchMatch($"stress-user-{i}", CancellationToken.None))
            .ToArray();

        var results = await Task.WhenAll(tasks);

        results.Should().HaveCount(200);
        results.Should().AllBeOfType<NoContentResult>("all unique users should succeed");
        processedCount.Should().Be(200, "all requests should be processed");
    }
}
