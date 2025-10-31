using FluentResults;
using MatchMaking.Shared.Models;

namespace MatchMaking.Service.Services.Abstracts
{
    public interface IKafkaService : IDisposable
    {
        Task<Result> PublishMatchRequestAsync(MatchRequest matchRequest, CancellationToken cancellationToken = default);

        Task<Result> StartConsumingAsync(CancellationToken stoppingToken);
    }
}
