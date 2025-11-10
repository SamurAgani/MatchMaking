using Confluent.Kafka;
using FluentAssertions;
using FluentResults;
using MatchMaking.Infrastructure.Kafka.Abstractions;
using MatchMaking.Service.DTOs;
using MatchMaking.Service.Services.Abstracts;
using MatchMaking.Service.Services.Concrete;
using MatchMaking.Shared.Models;
using Microsoft.Extensions.Logging;
using Moq;

namespace MatchMaking.Service.Test.UnitTests;

public class MatchmakingServiceTests
{
    private readonly Mock<IKafkaProducer<string, MatchRequest>> _mockKafkaProducer;
    private readonly Mock<IRateLimitService> _mockRateLimitService;
    private readonly Mock<IMatchStorageService> _mockMatchStorageService;
    private readonly Mock<ILogger<MatchmakingService>> _mockLogger;
    private readonly MatchmakingService _service;

    public MatchmakingServiceTests()
    {
        _mockKafkaProducer = new Mock<IKafkaProducer<string, MatchRequest>>();
        _mockRateLimitService = new Mock<IRateLimitService>();
        _mockMatchStorageService = new Mock<IMatchStorageService>();
        _mockLogger = new Mock<ILogger<MatchmakingService>>();

        _mockKafkaProducer.Setup(x => x.ProduceAsync(
            It.IsAny<string>(),
            It.IsAny<string>(),
            It.IsAny<MatchRequest>(),
            It.IsAny<CancellationToken>()))
            .ReturnsAsync(new DeliveryResult<string, MatchRequest>
            {
                Status = PersistenceStatus.Persisted,
                Offset = new Offset(0)
            });

        _service = new MatchmakingService(
            _mockKafkaProducer.Object,
            _mockRateLimitService.Object,
            _mockMatchStorageService.Object,
            _mockLogger.Object
        );
    }

    [Fact]
    public async Task SearchMatchAsync_WithValidUserId_ReturnsSuccess()
    {
        var userId = "user123";
        _mockMatchStorageService
            .Setup(x => x.GetMatchForUserAsync(userId, It.IsAny<CancellationToken>()))
            .ReturnsAsync(Result.Ok<MatchComplete?>(null));
        _mockRateLimitService
            .Setup(x => x.IsUserInQueueAsync(userId, It.IsAny<CancellationToken>()))
            .ReturnsAsync(Result.Ok(false));

        var result = await _service.SearchMatchAsync(userId, CancellationToken.None);

        result.IsSuccess.Should().BeTrue();
        _mockKafkaProducer.Verify(x => x.ProduceAsync(
            It.IsAny<string>(),
            It.Is<string>(k => k == userId),
            It.Is<MatchRequest>(m => m.UserId == userId),
            It.IsAny<CancellationToken>()),
            Times.Once);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public async Task SearchMatchAsync_WithInvalidUserId_ReturnsBadRequestError(string? userId)
    {
        var result = await _service.SearchMatchAsync(userId!, CancellationToken.None);

        result.IsFailed.Should().BeTrue();
        result.Errors.Should().ContainSingle();
        result.Errors[0].Message.Should().Be("UserId is required");
        result.Errors[0].Metadata.Should().ContainKey("status");
        result.Errors[0].Metadata["status"].Should().Be(400);
        _mockKafkaProducer.Verify(x => x.ProduceAsync(
            It.IsAny<string>(),
            It.IsAny<string>(),
            It.IsAny<MatchRequest>(),
            It.IsAny<CancellationToken>()),
            Times.Never);
    }

    [Fact]
    public async Task SearchMatchAsync_WhenUserHasActiveMatch_ReturnsConflictError()
    {
        var userId = "user123";
        var existingMatch = new MatchComplete("match-id-123", new List<string> { userId, "user2", "user3" });
        _mockMatchStorageService
            .Setup(x => x.GetMatchForUserAsync(userId, It.IsAny<CancellationToken>()))
            .ReturnsAsync(Result.Ok<MatchComplete?>(existingMatch));

        var result = await _service.SearchMatchAsync(userId, CancellationToken.None);

        result.IsFailed.Should().BeTrue();
        result.Errors.Should().ContainSingle();
        result.Errors[0].Message.Should().Be("You already have an active match");
        result.Errors[0].Metadata.Should().ContainKey("status");
        result.Errors[0].Metadata["status"].Should().Be(409);
        result.Errors[0].Metadata.Should().ContainKey("matchId");
        result.Errors[0].Metadata["matchId"].Should().Be("match-id-123");
        _mockKafkaProducer.Verify(x => x.ProduceAsync(
            It.IsAny<string>(),
            It.IsAny<string>(),
            It.IsAny<MatchRequest>(),
            It.IsAny<CancellationToken>()),
            Times.Never);
    }

    [Fact]
    public async Task SearchMatchAsync_WhenUserInQueue_ReturnsConflictError()
    {
        var userId = "user123";
        _mockMatchStorageService
            .Setup(x => x.GetMatchForUserAsync(userId, It.IsAny<CancellationToken>()))
            .ReturnsAsync(Result.Ok<MatchComplete?>(null));
        _mockRateLimitService
            .Setup(x => x.IsUserInQueueAsync(userId, It.IsAny<CancellationToken>()))
            .ReturnsAsync(Result.Ok(true));

        var result = await _service.SearchMatchAsync(userId, CancellationToken.None);

        result.IsFailed.Should().BeTrue();
        result.Errors.Should().ContainSingle();
        result.Errors[0].Message.Should().Be("You are already in the matchmaking queue");
        result.Errors[0].Metadata.Should().ContainKey("status");
        result.Errors[0].Metadata["status"].Should().Be(409);
        _mockKafkaProducer.Verify(x => x.ProduceAsync(
            It.IsAny<string>(),
            It.IsAny<string>(),
            It.IsAny<MatchRequest>(),
            It.IsAny<CancellationToken>()),
            Times.Never);
    }

    [Fact]
    public async Task SearchMatchAsync_WhenMatchStorageFails_ReturnsInternalServerError()
    {
        var userId = "user123";
        _mockMatchStorageService
            .Setup(x => x.GetMatchForUserAsync(userId, It.IsAny<CancellationToken>()))
            .ReturnsAsync(Result.Fail<MatchComplete?>("Redis connection failed"));

        var result = await _service.SearchMatchAsync(userId, CancellationToken.None);

        result.IsFailed.Should().BeTrue();
        result.Errors.Should().NotBeEmpty();
        result.Errors[0].Message.Should().Be("Failed to check existing match");
        result.Errors[0].Metadata.Should().ContainKey("status");
        result.Errors[0].Metadata["status"].Should().Be(500);
        _mockKafkaProducer.Verify(x => x.ProduceAsync(
            It.IsAny<string>(),
            It.IsAny<string>(),
            It.IsAny<MatchRequest>(),
            It.IsAny<CancellationToken>()),
            Times.Never);
    }

    [Fact]
    public async Task SearchMatchAsync_WhenRateLimitCheckFails_ReturnsInternalServerError()
    {
        var userId = "user123";
        _mockMatchStorageService
            .Setup(x => x.GetMatchForUserAsync(userId, It.IsAny<CancellationToken>()))
            .ReturnsAsync(Result.Ok<MatchComplete?>(null));
        _mockRateLimitService
            .Setup(x => x.IsUserInQueueAsync(userId, It.IsAny<CancellationToken>()))
            .ReturnsAsync(Result.Fail<bool>("Redis connection failed"));

        var result = await _service.SearchMatchAsync(userId, CancellationToken.None);

        result.IsFailed.Should().BeTrue();
        result.Errors.Should().NotBeEmpty();
        result.Errors[0].Message.Should().Be("Failed to check queue state");
        result.Errors[0].Metadata.Should().ContainKey("status");
        result.Errors[0].Metadata["status"].Should().Be(500);
        _mockKafkaProducer.Verify(x => x.ProduceAsync(
            It.IsAny<string>(),
            It.IsAny<string>(),
            It.IsAny<MatchRequest>(),
            It.IsAny<CancellationToken>()),
            Times.Never);
    }

    [Fact]
    public async Task SearchMatchAsync_WhenKafkaPublishFails_ReturnsInternalServerError()
    {
        var userId = "user123";
        _mockMatchStorageService
            .Setup(x => x.GetMatchForUserAsync(userId, It.IsAny<CancellationToken>()))
            .ReturnsAsync(Result.Ok<MatchComplete?>(null));
        _mockRateLimitService
            .Setup(x => x.IsUserInQueueAsync(userId, It.IsAny<CancellationToken>()))
            .ReturnsAsync(Result.Ok(false));
        _mockKafkaProducer
            .Setup(x => x.ProduceAsync(
                It.IsAny<string>(),
                It.IsAny<string>(),
                It.IsAny<MatchRequest>(),
                It.IsAny<CancellationToken>()))
            .ThrowsAsync(new ProduceException<string, MatchRequest>(
                new Confluent.Kafka.Error(ErrorCode.Local_MsgTimedOut),
                new DeliveryResult<string, MatchRequest>()));

        var result = await _service.SearchMatchAsync(userId, CancellationToken.None);

        result.IsFailed.Should().BeTrue();
        result.Errors.Should().NotBeEmpty();
        result.Errors[0].Message.Should().Be("Failed to enqueue matchmaking request");
        result.Errors[0].Metadata.Should().ContainKey("status");
        result.Errors[0].Metadata["status"].Should().Be(500);
    }

    [Fact]
    public async Task GetMatchAsync_WithExistingMatch_ReturnsSuccessWithMatchData()
    {
        var userId = "user123";
        var matchId = "match-id-123";
        var userIds = new List<string> { "user1", "user2", "user3" };
        var match = new MatchComplete(matchId, userIds);
        _mockMatchStorageService
            .Setup(x => x.GetMatchForUserAsync(userId, It.IsAny<CancellationToken>()))
            .ReturnsAsync(Result.Ok<MatchComplete?>(match));

        var result = await _service.GetMatchAsync(userId, CancellationToken.None);

        result.IsSuccess.Should().BeTrue();
        result.Value.Should().NotBeNull();
        result.Value.MatchId.Should().Be(matchId);
        result.Value.UserIds.Should().BeEquivalentTo(userIds);
    }

    [Fact]
    public async Task GetMatchAsync_WithNoMatch_ReturnsNotFoundError()
    {
        var userId = "user123";
        _mockMatchStorageService
            .Setup(x => x.GetMatchForUserAsync(userId, It.IsAny<CancellationToken>()))
            .ReturnsAsync(Result.Ok<MatchComplete?>(null));

        var result = await _service.GetMatchAsync(userId, CancellationToken.None);

        result.IsFailed.Should().BeTrue();
        result.Errors.Should().ContainSingle();
        result.Errors[0].Message.Should().Be("No match found for this user");
        result.Errors[0].Metadata.Should().ContainKey("status");
        result.Errors[0].Metadata["status"].Should().Be(404);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public async Task GetMatchAsync_WithInvalidUserId_ReturnsBadRequestError(string? userId)
    {
        var result = await _service.GetMatchAsync(userId!, CancellationToken.None);

        result.IsFailed.Should().BeTrue();
        result.Errors.Should().ContainSingle();
        result.Errors[0].Message.Should().Be("UserId is required");
        result.Errors[0].Metadata.Should().ContainKey("status");
        result.Errors[0].Metadata["status"].Should().Be(400);
    }

    [Fact]
    public async Task GetMatchAsync_WhenStorageFails_ReturnsInternalServerError()
    {
        var userId = "user123";
        _mockMatchStorageService
            .Setup(x => x.GetMatchForUserAsync(userId, It.IsAny<CancellationToken>()))
            .ReturnsAsync(Result.Fail<MatchComplete?>("Redis connection failed"));

        var result = await _service.GetMatchAsync(userId, CancellationToken.None);

        result.IsFailed.Should().BeTrue();
        result.Errors.Should().NotBeEmpty();
        result.Errors[0].Message.Should().Be("Failed to retrieve match information");
        result.Errors[0].Metadata.Should().ContainKey("status");
        result.Errors[0].Metadata["status"].Should().Be(500);
    }

    [Fact]
    public async Task GetMatchAsync_WithCancellationToken_PropagatesToken()
    {
        var userId = "user123";
        var cts = new CancellationTokenSource();
        var ct = cts.Token;
        var match = new MatchComplete("match-123", new List<string> { "user1", "user2" });
        _mockMatchStorageService
            .Setup(x => x.GetMatchForUserAsync(userId, ct))
            .ReturnsAsync(Result.Ok<MatchComplete?>(match));

        var result = await _service.GetMatchAsync(userId, ct);

        result.IsSuccess.Should().BeTrue();
        _mockMatchStorageService.Verify(x => x.GetMatchForUserAsync(userId, ct), Times.Once);
    }
}
