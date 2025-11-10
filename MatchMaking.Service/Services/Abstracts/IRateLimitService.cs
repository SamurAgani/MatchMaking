using FluentResults;

namespace MatchMaking.Service.Services.Abstracts
{
    public interface IRateLimitService
    {
        Task<Result<bool>> IsRateLimitedAsync(string userId, CancellationToken cancellationToken);

        Task<Result<bool>> IsUserInQueueAsync(string userId, CancellationToken cancellationToken);
    }
}
