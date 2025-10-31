using MatchMaking.Shared.Models;

namespace MatchMaking.Worker.Services.Abstracts
{
    public interface IKafkaService
    {
        Task StartConsumingAsync(CancellationToken cancellationToken);
        Task PublishMatchCompleteAsync(MatchComplete matchComplete, CancellationToken cancellationToken = default);
    }
}
