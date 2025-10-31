using FluentResults;

namespace MatchMaking.Service.Services.Abstracts
{
    public interface IMatchmakingService
    {
        Task<Result> SearchMatchAsync(string userId, CancellationToken ct = default);
        Task<Result<MatchDto>> GetMatchAsync(string userId, CancellationToken ct = default);
    }

    public sealed record MatchDto(string MatchId, IReadOnlyList<string> UserIds);
}
