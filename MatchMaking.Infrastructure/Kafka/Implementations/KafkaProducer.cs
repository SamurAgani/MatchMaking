using Confluent.Kafka;
using MatchMaking.Infrastructure.Kafka.Abstractions;
using MatchMaking.Infrastructure.Kafka.Serialization;
using Microsoft.Extensions.Logging;

namespace MatchMaking.Infrastructure.Kafka.Implementations;

public class KafkaProducer<TKey, TValue> : IKafkaProducer<TKey, TValue>
{
    private readonly IProducer<TKey, TValue> _producer;
    private readonly ILogger<KafkaProducer<TKey, TValue>> _logger;
    private bool _disposed;

    public KafkaProducer(
        ProducerConfig config,
        ILogger<KafkaProducer<TKey, TValue>> logger)
    {
        _logger = logger;

        var builder = new ProducerBuilder<TKey, TValue>(config);

        if (typeof(TValue) != typeof(string) && typeof(TValue) != typeof(byte[]))
        {
            builder.SetValueSerializer(new JsonSerializer<TValue>());
        }

        if (typeof(TKey) != typeof(string) && typeof(TKey) != typeof(byte[]))
        {
            builder.SetKeySerializer(new JsonSerializer<TKey>());
        }

        _producer = builder.Build();

        _logger.LogInformation(
            "KafkaProducer initialized for {KeyType}/{ValueType} with bootstrap servers: {BootstrapServers}",
            typeof(TKey).Name,
            typeof(TValue).Name,
            config.BootstrapServers);
    }
    public async Task<DeliveryResult<TKey, TValue>> ProduceAsync(
        string topic,
        TKey key,
        TValue value,
        CancellationToken cancellationToken)
    {
        ThrowIfDisposed();

        var message = new Message<TKey, TValue>
        {
            Key = key,
            Value = value
        };

        return await ProduceAsync(topic, message, cancellationToken);
    }

    public async Task<DeliveryResult<TKey, TValue>> ProduceAsync(
        string topic,
        Message<TKey, TValue> message,
        CancellationToken cancellationToken)
    {
        ThrowIfDisposed();

        try
        {
            var result = await _producer.ProduceAsync(topic, message, cancellationToken);

            _logger.LogDebug(
                "Message produced to topic {Topic} at offset {Offset}",
                topic,
                result.Offset);

            return result;
        }
        catch (ProduceException<TKey, TValue> ex)
        {
            _logger.LogError(
                ex,
                "Failed to produce message to topic {Topic}. Error: {ErrorReason}",
                topic,
                ex.Error.Reason);
            throw;
        }
    }

    public void Flush(TimeSpan timeout)
    {
        ThrowIfDisposed();
        _producer.Flush(timeout);
    }

    private void ThrowIfDisposed()
    {
        if (_disposed)
        {
            throw new ObjectDisposedException(nameof(KafkaProducer<TKey, TValue>));
        }
    }

    public void Dispose()
    {
        if (_disposed)
            return;

        try
        {
            _producer?.Flush(TimeSpan.FromSeconds(10));
            _producer?.Dispose();
            _logger.LogInformation("KafkaProducer disposed");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error disposing KafkaProducer");
        }
        finally
        {
            _disposed = true;
        }
    }
}
