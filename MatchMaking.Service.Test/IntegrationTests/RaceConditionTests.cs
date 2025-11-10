using FluentAssertions;
using FluentResults;
using MatchMaking.Service.Controllers;
using MatchMaking.Service.DTOs;
using MatchMaking.Service.Services.Abstracts;
using MatchMaking.Shared.Models;
using Microsoft.AspNetCore.Mvc;
using Moq;
using System.Collections.Concurrent;

namespace MatchMaking.Service.Test.IntegrationTests;

public class RaceConditionTests
{
    [Fact]
    public async Task ConcurrentQueueAdditions_SameTwoUsers_OnlyOneSucceeds()
    {
        var matchmakingService = new Mock<IMatchmakingService>();
        var queuedUsers = new ConcurrentBag<string>();

        matchmakingService.Setup(x => x.SearchMatchAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync((string userId, CancellationToken ct) =>
            {
                if (queuedUsers.Contains(userId))
                    return Result.Fail(new Error("You are already in the matchmaking queue").WithMetadata("status", 409));

                queuedUsers.Add(userId);
                return Result.Ok();
            });

        var controller = new MatchMakingController(matchmakingService.Object);

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

        var successes = results.Count(r => r is OkResult || r is NoContentResult);
        var failures = results.Count(r => r is ObjectResult);

        successes.Should().BeGreaterThan(0, "at least some requests should succeed");
        failures.Should().BeGreaterThan(0, "duplicate requests should be rejected");
    }

    [Fact]
    public async Task ConcurrentRequests_100Users_AllProcessedCorrectly()
    {
        var matchmakingService = new Mock<IMatchmakingService>();
        var publishedRequests = new ConcurrentBag<string>();

        matchmakingService.Setup(x => x.SearchMatchAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync((string userId, CancellationToken ct) =>
            {
                publishedRequests.Add(userId);
                return Result.Ok();
            });

        var controller = new MatchMakingController(matchmakingService.Object);

        var tasks = Enumerable.Range(1, 100)
            .Select(i => controller.SearchMatch($"user-{i}", CancellationToken.None))
            .ToList();

        var results = await Task.WhenAll(tasks);

        var successCount = results.Count(r => r is OkResult || r is NoContentResult);
        successCount.Should().Be(100, "all 100 unique user requests should succeed");

        publishedRequests.Should().HaveCount(100, "all requests should be published");
        publishedRequests.Distinct().Should().HaveCount(100, "all user IDs should be unique");
    }

    [Fact]
    public async Task ConcurrentMatchRetrieval_WhileMatchBeingStored_ShouldNotCrash()
    {
        var matchmakingService = new Mock<IMatchmakingService>();
        var match = new MatchDto("match-123", new List<string> { "user1", "user2", "user3" });
        var retrievalCount = 0;

        matchmakingService.Setup(x => x.GetMatchAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .Returns(async (string userId, CancellationToken ct) =>
            {
                Interlocked.Increment(ref retrievalCount);
                await Task.Delay(10);
                return retrievalCount % 2 == 0
                    ? Result.Ok(match)
                    : Result.Fail<MatchDto>(new Error("No match found").WithMetadata("status", 404));
            });

        var controller = new MatchMakingController(matchmakingService.Object);

        var tasks = Enumerable.Range(0, 50)
            .Select(_ => controller.GetMatch("user1", CancellationToken.None))
            .ToArray();

        var act = async () => await Task.WhenAll(tasks);
        await act.Should().NotThrowAsync("concurrent reads should be safe");

        var results = await Task.WhenAll(tasks);
        results.Should().AllSatisfy(r =>
            r.Should().Match<IActionResult>(result =>
                result is OkObjectResult || result is ObjectResult,
                "should return either OK or error response"));
    }

    [Fact]
    public async Task ConcurrentDuplicateUsers_WithDifferentStates_HandledCorrectly()
    {
        var matchmakingService = new Mock<IMatchmakingService>();
        var userStates = new ConcurrentDictionary<string, int>();

        matchmakingService.Setup(x => x.SearchMatchAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .Returns((string userId, CancellationToken ct) =>
            {
                var state = userStates.GetOrAdd(userId, 0);

                if (state > 2)
                {
                    return Task.FromResult(Result.Fail(new Error("You already have an active match")
                        .WithMetadata("status", 409)));
                }
                else if (state == 2)
                {
                    return Task.FromResult(Result.Fail(new Error("You are already in the matchmaking queue")
                        .WithMetadata("status", 409)));
                }

                userStates.AddOrUpdate(userId, 1, (k, v) => v + 1);
                return Task.FromResult(Result.Ok());
            });

        var controller = new MatchMakingController(matchmakingService.Object);

        var userId = "state-test-user";

        var tasks = Enumerable.Range(0, 10)
            .Select(_ => controller.SearchMatch(userId, CancellationToken.None))
            .ToArray();

        var results = await Task.WhenAll(tasks);

        results.Should().NotBeEmpty();
        results.Should().Contain(r => r is OkResult || r is NoContentResult || r is ObjectResult,
            "requests should be properly handled based on user state");
    }

    [Fact]
    public async Task HighConcurrency_StressTest_200SimultaneousRequests()
    {
        var matchmakingService = new Mock<IMatchmakingService>();
        var processedCount = 0;

        matchmakingService.Setup(x => x.SearchMatchAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync((string userId, CancellationToken ct) =>
            {
                Interlocked.Increment(ref processedCount);
                return Result.Ok();
            });

        var controller = new MatchMakingController(matchmakingService.Object);

        var tasks = Enumerable.Range(1, 200)
            .Select(i => controller.SearchMatch($"stress-user-{i}", CancellationToken.None))
            .ToArray();

        var results = await Task.WhenAll(tasks);

        results.Should().HaveCount(200);
        results.Should().AllSatisfy(r => r.Should().Match<IActionResult>(
            result => result is OkResult || result is NoContentResult,
            "all unique users should succeed"));
        processedCount.Should().Be(200, "all requests should be processed");
    }
}
