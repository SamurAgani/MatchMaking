using FluentAssertions;
using MatchMaking.Service.Controllers;
using MatchMaking.Service.Services.Abstracts;
using MatchMaking.Shared.Models;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using Moq;

namespace MatchMaking.Service.Test.UnitTests;

public class MatchMakingControllerTests
{
    private readonly Mock<IKafkaService> _mockKafkaService;
    private readonly Mock<IRateLimitService> _mockRateLimitService;
    private readonly Mock<IMatchStorageService> _mockMatchStorageService;
    private readonly Mock<ILogger<MatchMakingController>> _mockLogger;
    private readonly MatchMakingController _controller;

    public MatchMakingControllerTests()
    {
        _mockKafkaService = new Mock<IKafkaService>();
        _mockRateLimitService = new Mock<IRateLimitService>();
        _mockMatchStorageService = new Mock<IMatchStorageService>();
        _mockLogger = new Mock<ILogger<MatchMakingController>>();

        _controller = new MatchMakingController(
            _mockKafkaService.Object,
            _mockRateLimitService.Object,
            _mockMatchStorageService.Object,
            _mockLogger.Object
        );
    }

    [Fact]
    public async Task SearchMatch_WithValidUserId_ReturnsNoContent()
    {
        var userId = "user123";
        _mockMatchStorageService.Setup(x => x.GetMatchForUserAsync(userId, It.IsAny<CancellationToken>()))
            .ReturnsAsync((MatchComplete?)null);
        _mockRateLimitService.Setup(x => x.IsUserInQueueAsync(userId, It.IsAny<CancellationToken>()))
            .ReturnsAsync(false);
        _mockRateLimitService.Setup(x => x.IsRateLimitedAsync(userId, It.IsAny<CancellationToken>()))
            .ReturnsAsync(false);

        var result = await _controller.SearchMatch(userId, CancellationToken.None);

        result.Should().BeOfType<NoContentResult>();
        _mockKafkaService.Verify(x => x.PublishMatchRequestAsync(
            It.Is<MatchRequest>(m => m.UserId == userId),
            It.IsAny<CancellationToken>()),
            Times.Once);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public async Task SearchMatch_WithInvalidUserId_ReturnsBadRequest(string? userId)
    {
        var result = await _controller.SearchMatch(userId!, CancellationToken.None);

        var badRequestResult = result.Should().BeOfType<BadRequestObjectResult>().Subject;
        badRequestResult.Value.Should().NotBeNull();
        _mockKafkaService.Verify(x => x.PublishMatchRequestAsync(
            It.IsAny<MatchRequest>(),
            It.IsAny<CancellationToken>()),
            Times.Never);
    }

    [Fact]
    public async Task SearchMatch_WhenUserHasActiveMatch_ReturnsBadRequest()
    {
        var userId = "user123";
        var existingMatch = new MatchComplete("match-id-123", new List<string> { userId, "user2", "user3" });
        _mockMatchStorageService.Setup(x => x.GetMatchForUserAsync(userId, It.IsAny<CancellationToken>()))
            .ReturnsAsync(existingMatch);

        var result = await _controller.SearchMatch(userId, CancellationToken.None);

        var badRequestResult = result.Should().BeOfType<BadRequestObjectResult>().Subject;
        badRequestResult.Value.Should().NotBeNull();
        _mockKafkaService.Verify(x => x.PublishMatchRequestAsync(
            It.IsAny<MatchRequest>(),
            It.IsAny<CancellationToken>()),
            Times.Never);
    }

    [Fact]
    public async Task SearchMatch_WhenUserInQueue_ReturnsBadRequest()
    {
        var userId = "user123";
        _mockMatchStorageService.Setup(x => x.GetMatchForUserAsync(userId, It.IsAny<CancellationToken>()))
            .ReturnsAsync((MatchComplete?)null);
        _mockRateLimitService.Setup(x => x.IsUserInQueueAsync(userId, It.IsAny<CancellationToken>()))
            .ReturnsAsync(true);

        var result = await _controller.SearchMatch(userId, CancellationToken.None);

        var badRequestResult = result.Should().BeOfType<BadRequestObjectResult>().Subject;
        badRequestResult.Value.Should().NotBeNull();
        _mockKafkaService.Verify(x => x.PublishMatchRequestAsync(
            It.IsAny<MatchRequest>(),
            It.IsAny<CancellationToken>()),
            Times.Never);
    }

    [Fact]
    public async Task SearchMatch_WhenRateLimited_ReturnsBadRequest()
    {
        var userId = "user123";
        _mockMatchStorageService.Setup(x => x.GetMatchForUserAsync(userId, It.IsAny<CancellationToken>()))
            .ReturnsAsync((MatchComplete?)null);
        _mockRateLimitService.Setup(x => x.IsUserInQueueAsync(userId, It.IsAny<CancellationToken>()))
            .ReturnsAsync(false);
        _mockRateLimitService.Setup(x => x.IsRateLimitedAsync(userId, It.IsAny<CancellationToken>()))
            .ReturnsAsync(true);

        var result = await _controller.SearchMatch(userId, CancellationToken.None);

        var badRequestResult = result.Should().BeOfType<BadRequestObjectResult>().Subject;
        badRequestResult.Value.Should().NotBeNull();
        _mockKafkaService.Verify(x => x.PublishMatchRequestAsync(
            It.IsAny<MatchRequest>(),
            It.IsAny<CancellationToken>()),
            Times.Never);
    }

    [Fact]
    public async Task SearchMatch_WhenKafkaFails_ReturnsInternalServerError()
    {
        var userId = "user123";
        _mockMatchStorageService.Setup(x => x.GetMatchForUserAsync(userId, It.IsAny<CancellationToken>()))
            .ReturnsAsync((MatchComplete?)null);
        _mockRateLimitService.Setup(x => x.IsUserInQueueAsync(userId, It.IsAny<CancellationToken>()))
            .ReturnsAsync(false);
        _mockRateLimitService.Setup(x => x.IsRateLimitedAsync(userId, It.IsAny<CancellationToken>()))
            .ReturnsAsync(false);
        _mockKafkaService.Setup(x => x.PublishMatchRequestAsync(It.IsAny<MatchRequest>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new Exception("Kafka connection failed"));

        var result = await _controller.SearchMatch(userId, CancellationToken.None);

        var statusCodeResult = result.Should().BeOfType<ObjectResult>().Subject;
        statusCodeResult.StatusCode.Should().Be(500);
    }

    [Fact]
    public async Task GetMatch_WithExistingMatch_ReturnsOkWithMatchData()
    {
        var userId = "user123";
        var matchId = "match-id-123";
        var userIds = new List<string> { "user1", "user2", "user3" };
        var match = new MatchComplete(matchId, userIds);
        _mockMatchStorageService.Setup(x => x.GetMatchForUserAsync(userId, It.IsAny<CancellationToken>()))
            .ReturnsAsync(match);

        var result = await _controller.GetMatch(userId, CancellationToken.None);

        var okResult = result.Should().BeOfType<OkObjectResult>().Subject;
        okResult.Value.Should().NotBeNull();
    }

    [Fact]
    public async Task GetMatch_WithNoMatch_ReturnsNotFound()
    {
        var userId = "user123";
        _mockMatchStorageService.Setup(x => x.GetMatchForUserAsync(userId, It.IsAny<CancellationToken>()))
            .ReturnsAsync((MatchComplete?)null);

        var result = await _controller.GetMatch(userId, CancellationToken.None);

        var notFoundResult = result.Should().BeOfType<NotFoundObjectResult>().Subject;
        notFoundResult.Value.Should().NotBeNull();
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public async Task GetMatch_WithInvalidUserId_ReturnsBadRequest(string? userId)
    {
        var result = await _controller.GetMatch(userId!, CancellationToken.None);

        result.Should().BeOfType<BadRequestObjectResult>();
    }

    [Fact]
    public async Task GetMatch_WhenStorageFails_ReturnsInternalServerError()
    {
        var userId = "user123";
        _mockMatchStorageService.Setup(x => x.GetMatchForUserAsync(userId, It.IsAny<CancellationToken>()))
            .ThrowsAsync(new Exception("Redis connection failed"));

        var result = await _controller.GetMatch(userId, CancellationToken.None);

        var statusCodeResult = result.Should().BeOfType<ObjectResult>().Subject;
        statusCodeResult.StatusCode.Should().Be(500);
    }

    [Fact]
    public void HealthCheck_ReturnsOk()
    {
        var result = _controller.HealthCheck();

        var okResult = result.Should().BeOfType<OkObjectResult>().Subject;
        okResult.Value.Should().NotBeNull();
    }
}
