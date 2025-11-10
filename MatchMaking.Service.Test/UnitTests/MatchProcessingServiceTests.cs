using Confluent.Kafka;
using FluentAssertions;
using FluentResults;
using MatchMaking.Service.Services.Abstracts;
using MatchMaking.Service.Services.Concrete;
using MatchMaking.Shared.Models;
using Microsoft.Extensions.Logging;
using Moq;

namespace MatchMaking.Service.Test.UnitTests;

public class MatchProcessingServiceTests
{
    private readonly Mock<IMatchStorageService> _mockMatchStorageService;
    private readonly Mock<ILogger<MatchProcessingService>> _mockLogger;
    private readonly MatchProcessingService _service;

    public MatchProcessingServiceTests()
    {
        _mockMatchStorageService = new Mock<IMatchStorageService>();
        _mockLogger = new Mock<ILogger<MatchProcessingService>>();

        _service = new MatchProcessingService(
            _mockMatchStorageService.Object,
            _mockLogger.Object
        );
    }

    [Fact]
    public async Task ProcessMatchCompleteMessageAsync_WithValidMessage_ReturnsTrue()
    {
        var matchId = "match-123";
        var userIds = new List<string> { "user1", "user2", "user3" };
        var matchComplete = new MatchComplete(matchId, userIds);
        var message = new Message<string, MatchComplete>
        {
            Key = matchId,
            Value = matchComplete
        };
        var consumeResult = new ConsumeResult<string, MatchComplete>
        {
            Message = message
        };

        _mockMatchStorageService
            .Setup(x => x.StoreMatchForUserAsync(It.IsAny<string>(), It.IsAny<MatchComplete>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(Result.Ok());

        var result = await _service.ProcessMatchCompleteMessageAsync(consumeResult, CancellationToken.None);

        result.Should().BeTrue("all users should be stored successfully");
        _mockMatchStorageService.Verify(
            x => x.StoreMatchForUserAsync(It.IsAny<string>(), matchComplete, It.IsAny<CancellationToken>()),
            Times.Exactly(3),
            "should store match for all 3 users");
    }

    [Fact]
    public async Task ProcessMatchCompleteMessageAsync_WithNullMessage_ReturnsFalse()
    {
        var message = new Message<string, MatchComplete>
        {
            Key = "match-123",
            Value = null!
        };
        var consumeResult = new ConsumeResult<string, MatchComplete>
        {
            Message = message
        };

        var result = await _service.ProcessMatchCompleteMessageAsync(consumeResult, CancellationToken.None);

        result.Should().BeFalse("null message should fail");
        _mockMatchStorageService.Verify(
            x => x.StoreMatchForUserAsync(It.IsAny<string>(), It.IsAny<MatchComplete>(), It.IsAny<CancellationToken>()),
            Times.Never,
            "should not attempt to store anything for null message");
    }

    [Fact]
    public async Task ProcessMatchCompleteMessageAsync_StoresMatchForEachUser()
    {
        var matchId = "match-456";
        var userIds = new List<string> { "user1", "user2", "user3", "user4" };
        var matchComplete = new MatchComplete(matchId, userIds);
        var message = new Message<string, MatchComplete>
        {
            Key = matchId,
            Value = matchComplete
        };
        var consumeResult = new ConsumeResult<string, MatchComplete>
        {
            Message = message
        };

        _mockMatchStorageService
            .Setup(x => x.StoreMatchForUserAsync(It.IsAny<string>(), It.IsAny<MatchComplete>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(Result.Ok());

        var result = await _service.ProcessMatchCompleteMessageAsync(consumeResult, CancellationToken.None);

        result.Should().BeTrue();
        foreach (var userId in userIds)
        {
            _mockMatchStorageService.Verify(
                x => x.StoreMatchForUserAsync(userId, matchComplete, It.IsAny<CancellationToken>()),
                Times.Once,
                $"should store match for user {userId}");
        }
    }

    [Fact]
    public async Task ProcessMatchCompleteMessageAsync_WhenStorageFails_ReturnsFalse()
    {
        var matchId = "match-789";
        var userIds = new List<string> { "user1", "user2" };
        var matchComplete = new MatchComplete(matchId, userIds);
        var message = new Message<string, MatchComplete>
        {
            Key = matchId,
            Value = matchComplete
        };
        var consumeResult = new ConsumeResult<string, MatchComplete>
        {
            Message = message
        };

        _mockMatchStorageService
            .Setup(x => x.StoreMatchForUserAsync(It.IsAny<string>(), It.IsAny<MatchComplete>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(Result.Fail("Redis connection failed"));

        var result = await _service.ProcessMatchCompleteMessageAsync(consumeResult, CancellationToken.None);

        result.Should().BeFalse("storage failure should result in false");
    }

    [Fact]
    public async Task ProcessMatchCompleteMessageAsync_WhenPartialStorageFailure_ReturnsFalse()
    {
        var matchId = "match-partial";
        var userIds = new List<string> { "user1", "user2", "user3" };
        var matchComplete = new MatchComplete(matchId, userIds);
        var message = new Message<string, MatchComplete>
        {
            Key = matchId,
            Value = matchComplete
        };
        var consumeResult = new ConsumeResult<string, MatchComplete>
        {
            Message = message
        };

        _mockMatchStorageService
            .Setup(x => x.StoreMatchForUserAsync("user1", It.IsAny<MatchComplete>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(Result.Ok());
        _mockMatchStorageService
            .Setup(x => x.StoreMatchForUserAsync("user2", It.IsAny<MatchComplete>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(Result.Fail("Storage failed for user2"));
        _mockMatchStorageService
            .Setup(x => x.StoreMatchForUserAsync("user3", It.IsAny<MatchComplete>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(Result.Ok());

        var result = await _service.ProcessMatchCompleteMessageAsync(consumeResult, CancellationToken.None);

        result.Should().BeFalse("partial failure should result in false");
        _mockMatchStorageService.Verify(
            x => x.StoreMatchForUserAsync(It.IsAny<string>(), It.IsAny<MatchComplete>(), It.IsAny<CancellationToken>()),
            Times.Exactly(3),
            "should attempt to store for all users despite one failure");
    }

    [Fact]
    public async Task ProcessMatchCompleteMessageAsync_WhenExceptionThrown_ReturnsFalse()
    {
        var matchId = "match-exception";
        var userIds = new List<string> { "user1" };
        var matchComplete = new MatchComplete(matchId, userIds);
        var message = new Message<string, MatchComplete>
        {
            Key = matchId,
            Value = matchComplete
        };
        var consumeResult = new ConsumeResult<string, MatchComplete>
        {
            Message = message
        };

        _mockMatchStorageService
            .Setup(x => x.StoreMatchForUserAsync(It.IsAny<string>(), It.IsAny<MatchComplete>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new Exception("Unexpected error"));

        var result = await _service.ProcessMatchCompleteMessageAsync(consumeResult, CancellationToken.None);

        result.Should().BeFalse("exception should be caught and return false");
    }

    [Fact]
    public async Task ProcessMatchCompleteMessageAsync_WithEmptyUserList_ReturnsTrue()
    {
        var matchId = "match-empty";
        var userIds = new List<string>();
        var matchComplete = new MatchComplete(matchId, userIds);
        var message = new Message<string, MatchComplete>
        {
            Key = matchId,
            Value = matchComplete
        };
        var consumeResult = new ConsumeResult<string, MatchComplete>
        {
            Message = message
        };

        var result = await _service.ProcessMatchCompleteMessageAsync(consumeResult, CancellationToken.None);

        result.Should().BeTrue("empty user list should succeed without errors");
        _mockMatchStorageService.Verify(
            x => x.StoreMatchForUserAsync(It.IsAny<string>(), It.IsAny<MatchComplete>(), It.IsAny<CancellationToken>()),
            Times.Never,
            "should not attempt to store anything with empty user list");
    }

    [Fact]
    public async Task ProcessMatchCompleteMessageAsync_WithSingleUser_ReturnsTrue()
    {
        var matchId = "match-single";
        var userIds = new List<string> { "solo-user" };
        var matchComplete = new MatchComplete(matchId, userIds);
        var message = new Message<string, MatchComplete>
        {
            Key = matchId,
            Value = matchComplete
        };
        var consumeResult = new ConsumeResult<string, MatchComplete>
        {
            Message = message
        };

        _mockMatchStorageService
            .Setup(x => x.StoreMatchForUserAsync(It.IsAny<string>(), It.IsAny<MatchComplete>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(Result.Ok());

        var result = await _service.ProcessMatchCompleteMessageAsync(consumeResult, CancellationToken.None);

        result.Should().BeTrue();
        _mockMatchStorageService.Verify(
            x => x.StoreMatchForUserAsync("solo-user", matchComplete, It.IsAny<CancellationToken>()),
            Times.Once,
            "should store match for the single user");
    }

    [Fact]
    public async Task ProcessMatchCompleteMessageAsync_WithCancellationToken_PropagatesToken()
    {
        var matchId = "match-cancel";
        var userIds = new List<string> { "user1" };
        var matchComplete = new MatchComplete(matchId, userIds);
        var message = new Message<string, MatchComplete>
        {
            Key = matchId,
            Value = matchComplete
        };
        var consumeResult = new ConsumeResult<string, MatchComplete>
        {
            Message = message
        };

        var cts = new CancellationTokenSource();
        var ct = cts.Token;

        _mockMatchStorageService
            .Setup(x => x.StoreMatchForUserAsync(It.IsAny<string>(), It.IsAny<MatchComplete>(), ct))
            .ReturnsAsync(Result.Ok());

        var result = await _service.ProcessMatchCompleteMessageAsync(consumeResult, ct);

        result.Should().BeTrue();
        _mockMatchStorageService.Verify(
            x => x.StoreMatchForUserAsync(It.IsAny<string>(), It.IsAny<MatchComplete>(), ct),
            Times.Once,
            "should propagate the cancellation token");
    }

    [Fact]
    public async Task ProcessMatchCompleteMessageAsync_LogsSuccessfulStorage()
    {
        var matchId = "match-log";
        var userIds = new List<string> { "user1", "user2" };
        var matchComplete = new MatchComplete(matchId, userIds);
        var message = new Message<string, MatchComplete>
        {
            Key = matchId,
            Value = matchComplete
        };
        var consumeResult = new ConsumeResult<string, MatchComplete>
        {
            Message = message
        };

        _mockMatchStorageService
            .Setup(x => x.StoreMatchForUserAsync(It.IsAny<string>(), It.IsAny<MatchComplete>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(Result.Ok());

        var result = await _service.ProcessMatchCompleteMessageAsync(consumeResult, CancellationToken.None);

        result.Should().BeTrue();
        _mockLogger.Verify(
            x => x.Log(
                LogLevel.Information,
                It.IsAny<EventId>(),
                It.Is<It.IsAnyType>((v, t) => v.ToString()!.Contains("Processing match")),
                null,
                It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
            Times.AtLeastOnce,
            "should log processing information");
    }

    [Fact]
    public async Task ProcessMatchCompleteMessageAsync_WithMultipleFailures_ReturnsAllErrors()
    {
        var matchId = "match-multi-fail";
        var userIds = new List<string> { "user1", "user2", "user3" };
        var matchComplete = new MatchComplete(matchId, userIds);
        var message = new Message<string, MatchComplete>
        {
            Key = matchId,
            Value = matchComplete
        };
        var consumeResult = new ConsumeResult<string, MatchComplete>
        {
            Message = message
        };

        _mockMatchStorageService
            .Setup(x => x.StoreMatchForUserAsync("user1", It.IsAny<MatchComplete>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(Result.Fail("Error 1"));
        _mockMatchStorageService
            .Setup(x => x.StoreMatchForUserAsync("user2", It.IsAny<MatchComplete>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(Result.Fail("Error 2"));
        _mockMatchStorageService
            .Setup(x => x.StoreMatchForUserAsync("user3", It.IsAny<MatchComplete>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(Result.Fail("Error 3"));

        var result = await _service.ProcessMatchCompleteMessageAsync(consumeResult, CancellationToken.None);

        result.Should().BeFalse("all storage operations failed");
        _mockMatchStorageService.Verify(
            x => x.StoreMatchForUserAsync(It.IsAny<string>(), It.IsAny<MatchComplete>(), It.IsAny<CancellationToken>()),
            Times.Exactly(3),
            "should attempt all operations even when they fail");
    }
}
