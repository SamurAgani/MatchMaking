using System.Net;
using Confluent.Kafka;
using FluentResults;
using MatchMaking.Infrastructure.Kafka.Abstractions;
using MatchMaking.Service.DTOs;
using MatchMaking.Service.Services.Abstracts;
using MatchMaking.Shared.Constants;
using MatchMaking.Shared.Models;

namespace MatchMaking.Service.Services.Concrete;

public class MatchmakingService(
    IKafkaProducer<string, MatchRequest> kafkaProducer,
    IRateLimitService rateLimitService,
    IMatchStorageService matchStorageService,
    ILogger<MatchmakingService> logger) : IMatchmakingService
{
    private readonly IKafkaProducer<string, MatchRequest> _kafkaProducer = kafkaProducer;
    private readonly IRateLimitService _rateLimitService = rateLimitService;
    private readonly IMatchStorageService _matchStorageService = matchStorageService;
    private readonly ILogger<MatchmakingService> _logger = logger;

    public async Task<Result> SearchMatchAsync(string userId, CancellationToken ct)
    {
        if (string.IsNullOrWhiteSpace(userId))
            return Result.Fail(new FluentResults.Error("UserId is required").WithMetadata(MetadataKeys.Status, (int)HttpStatusCode.BadRequest));

        var existingMatchRes = await _matchStorageService.GetMatchForUserAsync(userId, ct);
        if (existingMatchRes.IsFailed)
            return Fail500("Failed to check existing match", existingMatchRes.ToResult());

        if (existingMatchRes.Value is not null)
            return Result.Fail(new FluentResults.Error("You already have an active match")
                .WithMetadata(MetadataKeys.Status, (int)HttpStatusCode.Conflict)
                .WithMetadata(MetadataKeys.MatchId, existingMatchRes.Value.MatchId));

        var inQueueRes = await _rateLimitService.IsUserInQueueAsync(userId, ct);
        if (inQueueRes.IsFailed)
            return Fail500("Failed to check queue state", inQueueRes.ToResult());

        if (inQueueRes.Value)
            return Result.Fail(new FluentResults.Error("You are already in the matchmaking queue")
                .WithMetadata(MetadataKeys.Status, (int)HttpStatusCode.Conflict));

        try
        {
            var matchRequest = new MatchRequest(userId);
            await _kafkaProducer.ProduceAsync(
                KafkaTopics.MatchmakingRequest,
                matchRequest.UserId,
                matchRequest,
                ct);

            _logger.LogInformation("Published match request for user {UserId}", userId);
            return Result.Ok();
        }
        catch (ProduceException<string, MatchRequest> ex)
        {
            _logger.LogError(ex, "Failed to publish match request for user {UserId}", userId);
            return Fail500("Failed to enqueue matchmaking request", Result.Fail(ex.Message));
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Unexpected error while publishing match request for user {UserId}", userId);
            return Fail500("Unexpected error while enqueuing matchmaking request", Result.Fail(ex.Message));
        }
    }

    public async Task<Result<MatchDto>> GetMatchAsync(string userId, CancellationToken ct)
    {
        if (string.IsNullOrWhiteSpace(userId))
            return Result.Fail<MatchDto>(new FluentResults.Error("UserId is required").WithMetadata(MetadataKeys.Status, (int)HttpStatusCode.BadRequest));

        var matchRes = await _matchStorageService.GetMatchForUserAsync(userId, ct);
        if (matchRes.IsFailed)
            return Fail500<MatchDto>("Failed to retrieve match information", matchRes.ToResult());

        if (matchRes.Value is null)
            return Result.Fail<MatchDto>(new FluentResults.Error("No match found for this user").WithMetadata(MetadataKeys.Status, (int)HttpStatusCode.NotFound));

        var dto = new MatchDto(matchRes.Value.MatchId, matchRes.Value.UserIds);
        return Result.Ok(dto);
    }

    private static Result Fail500(string message, Result inner) =>
        Result.Fail(new FluentResults.Error(message).WithMetadata(MetadataKeys.Status, (int)HttpStatusCode.InternalServerError).CausedBy(inner.Errors));

    private static Result<T> Fail500<T>(string message, Result inner) =>
        Result.Fail<T>(new FluentResults.Error(message).WithMetadata(MetadataKeys.Status, (int)HttpStatusCode.InternalServerError).CausedBy(inner.Errors));
}
