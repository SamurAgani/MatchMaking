using Confluent.Kafka;

namespace MatchMaking.Infrastructure.Kafka.Abstractions;

public interface IKafkaProducer<TKey, TValue> : IDisposable
{
    Task<DeliveryResult<TKey, TValue>> ProduceAsync(
        string topic,
        TKey key,
        TValue value,
        CancellationToken cancellationToken);

    Task<DeliveryResult<TKey, TValue>> ProduceAsync(
        string topic,
        Message<TKey, TValue> message,
        CancellationToken cancellationToken);

    void Flush(TimeSpan timeout);
}
