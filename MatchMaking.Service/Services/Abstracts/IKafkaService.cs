using MatchMaking.Shared.Models;

namespace MatchMaking.Service.Services.Abstracts
{
    public interface IKafkaService
    {
        Task PublishMatchRequestAsync(MatchRequest matchRequest, CancellationToken cancellationToken = default);

        Task StartConsumingAsync(CancellationToken stoppingToken);
    }
}
