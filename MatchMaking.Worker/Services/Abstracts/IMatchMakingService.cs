using MatchMaking.Shared.Models;

namespace MatchMaking.Worker.Services.Abstracts
{
    public interface IMatchMakingService
    {
        Task<MatchComplete?> TryCreateMatchAsync(string userId, CancellationToken cancellationToken = default);
    }
}
