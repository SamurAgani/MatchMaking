using Confluent.Kafka;
using MatchMaking.Shared.Models;

namespace MatchMaking.Service.Services.Abstracts;

public interface IMatchProcessingService
{
    Task<bool> ProcessMatchCompleteMessageAsync(
        ConsumeResult<string, MatchComplete> consumeResult,
        CancellationToken stoppingToken);
}
