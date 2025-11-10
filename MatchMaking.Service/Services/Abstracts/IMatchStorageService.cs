using FluentResults;
using MatchMaking.Shared.Models;

namespace MatchMaking.Service.Services.Abstracts
{
    public interface IMatchStorageService
    {
        Task<Result> StoreMatchForUserAsync(string userId, MatchComplete matchComplete, CancellationToken cancellationToken);

        Task<Result<MatchComplete?>> GetMatchForUserAsync(string userId, CancellationToken cancellationToken);
    }
}
