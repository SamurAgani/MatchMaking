using FluentResults;
using MatchMaking.Service.Services.Abstracts;
using MatchMaking.Shared.Models;

namespace MatchMaking.Service.Services.Concrete
{
    public class MatchmakingService : IMatchmakingService
    {
        private readonly IKafkaService _kafkaService;
        private readonly IRateLimitService _rateLimitService;
        private readonly IMatchStorageService _matchStorageService;
        private readonly ILogger<MatchmakingService> _logger;

        public MatchmakingService(
            IKafkaService kafkaService,
            IRateLimitService rateLimitService,
            IMatchStorageService matchStorageService,
            ILogger<MatchmakingService> logger)
        {
            _kafkaService = kafkaService;
            _rateLimitService = rateLimitService;
            _matchStorageService = matchStorageService;
            _logger = logger;
        }

        public async Task<Result> SearchMatchAsync(string userId, CancellationToken ct = default)
        {
            if (string.IsNullOrWhiteSpace(userId))
                return Result.Fail(new Error("UserId is required").WithMetadata("status", 400));

            var existingMatchRes = await _matchStorageService.GetMatchForUserAsync(userId, ct);
            if (existingMatchRes.IsFailed)
                return Fail500("Failed to check existing match", existingMatchRes.ToResult());

            if (existingMatchRes.Value is not null)
                return Result.Fail(new Error("You already have an active match")
                    .WithMetadata("status", 409)
                    .WithMetadata("matchId", existingMatchRes.Value.MatchId));

            var inQueueRes = await _rateLimitService.IsUserInQueueAsync(userId, ct);
            if (inQueueRes.IsFailed)
                return Fail500("Failed to check queue state", inQueueRes.ToResult());

            if (inQueueRes.Value)
                return Result.Fail(new Error("You are already in the matchmaking queue")
                    .WithMetadata("status", 409));

            var publishRes = await _kafkaService.PublishMatchRequestAsync(new MatchRequest(userId), ct);
            if (publishRes.IsFailed)
                return Fail500("Failed to enqueue matchmaking request", publishRes);

            return Result.Ok();
        }

        public async Task<Result<MatchDto>> GetMatchAsync(string userId, CancellationToken ct = default)
        {
            if (string.IsNullOrWhiteSpace(userId))
                return Result.Fail<MatchDto>(new Error("UserId is required").WithMetadata("status", 400));

            var matchRes = await _matchStorageService.GetMatchForUserAsync(userId, ct);
            if (matchRes.IsFailed)
                return Fail500<MatchDto>("Failed to retrieve match information", matchRes.ToResult());

            if (matchRes.Value is null)
                return Result.Fail<MatchDto>(new Error("No match found for this user").WithMetadata("status", 404));

            var dto = new MatchDto(matchRes.Value.MatchId, matchRes.Value.UserIds);
            return Result.Ok(dto);
        }

        private static Result Fail500(string message, Result inner) =>
            Result.Fail(new Error(message).WithMetadata("status", 500).CausedBy(inner.Errors));

        private static Result<T> Fail500<T>(string message, Result inner) =>
            Result.Fail<T>(new Error(message).WithMetadata("status", 500).CausedBy(inner.Errors));
    }
}
