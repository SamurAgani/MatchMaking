using MatchMaking.Shared.Models;

namespace MatchMaking.Service.Services.Abstracts
{
    public interface IMatchStorageService
    {
        Task StoreMatchForUserAsync(string userId, MatchComplete matchComplete, CancellationToken cancellationToken = default);
        Task<MatchComplete?> GetMatchForUserAsync(string userId, CancellationToken cancellationToken = default);
    }
}
