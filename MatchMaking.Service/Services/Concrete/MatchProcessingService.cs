using Confluent.Kafka;
using FluentResults;
using MatchMaking.Service.Services.Abstracts;
using MatchMaking.Shared.Models;

namespace MatchMaking.Service.Services.Concrete;

public class MatchProcessingService(
    IMatchStorageService matchStorageService,
    ILogger<MatchProcessingService> logger) : IMatchProcessingService
{
    private readonly IMatchStorageService _matchStorageService = matchStorageService;
    private readonly ILogger<MatchProcessingService> _logger = logger;

    public async Task<bool> ProcessMatchCompleteMessageAsync(
    ConsumeResult<string, MatchComplete> consumeResult,
    CancellationToken stoppingToken)
    {
        try
        {
            var matchComplete = consumeResult.Message.Value;

            if (matchComplete is null)
            {
                _logger.LogWarning("MatchComplete was null after deserialization");
                return false;
            }

            var processResult = await ProcessMatchCompleteAsync(matchComplete, stoppingToken);

            if (processResult.IsFailed)
            {
                _logger.LogWarning(
                    "Processing MatchComplete failed: {Errors}",
                    string.Join(" | ", processResult.Errors.Select(e => e.Message)));
                return false;
            }

            return true;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error processing match complete message");
            return false;
        }
    }

    private async Task<Result> ProcessMatchCompleteAsync(MatchComplete matchComplete, CancellationToken cancellationToken)
    {
        _logger.LogInformation(
            "Processing match {MatchId} with {PlayerCount} players",
            matchComplete.MatchId,
            matchComplete.UserIds.Count);

        var failures = new List<IError>();

        foreach (var userId in matchComplete.UserIds)
        {
            var storeResult = await StoreMatchForUserAsync(userId, matchComplete, cancellationToken);
            if (storeResult.IsFailed)
            {
                failures.AddRange(storeResult.Errors);
            }
        }

        return failures.Count == 0
            ? Result.Ok()
            : Result.Fail(failures);
    }

    private async Task<Result> StoreMatchForUserAsync(string userId, MatchComplete matchComplete, CancellationToken cancellationToken)
    {
        var store = await _matchStorageService.StoreMatchForUserAsync(userId, matchComplete, cancellationToken);

        if (store.IsSuccess)
        {
            _logger.LogInformation(
                "Stored match {MatchId} for user {UserId}",
                matchComplete.MatchId,
                userId);
            return Result.Ok();
        }

        foreach (var err in store.Errors)
        {
            _logger.LogError("Failed to store match {MatchId} for user {UserId}: {Error}", matchComplete.MatchId, userId, err.Message);
        }

        return store;
    }
}
