namespace MatchMaking.Service.Services.Abstracts
{
    public interface IRateLimitService
    {
        Task<bool> IsRateLimitedAsync(string userId, CancellationToken cancellationToken = default);
        Task<bool> IsUserInQueueAsync(string userId, CancellationToken cancellationToken = default);
    }
}
