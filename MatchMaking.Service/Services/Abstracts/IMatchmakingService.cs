using FluentResults;
using MatchMaking.Service.DTOs;

namespace MatchMaking.Service.Services.Abstracts
{
    public interface IMatchmakingService
    {
        Task<Result> SearchMatchAsync(string userId, CancellationToken ct);
        Task<Result<MatchDto>> GetMatchAsync(string userId, CancellationToken ct);
    }
}
