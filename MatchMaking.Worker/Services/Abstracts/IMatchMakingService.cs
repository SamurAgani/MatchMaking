using Confluent.Kafka;
using MatchMaking.Shared.Models;

namespace MatchMaking.Worker.Services.Abstracts
{
    public interface IMatchMakingService
    {
        Task<bool> ProcessMatchRequestAsync(
           ConsumeResult<string, MatchRequest> consumeResult,
           CancellationToken cancellationToken);
    }
}
