using Confluent.Kafka;

namespace MatchMaking.Infrastructure.Kafka.Abstractions;

public interface IKafkaConsumer<TKey, TValue> : IDisposable
{
    void Subscribe(string topic);
    void Subscribe(IEnumerable<string> topics);

    Task StartConsumingAsync(
        Func<ConsumeResult<TKey, TValue>, CancellationToken, Task<bool>> messageHandler,
        CancellationToken cancellationToken);

    void Close();
}
