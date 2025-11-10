using Confluent.Kafka;
using MatchMaking.Infrastructure.Kafka.Abstractions;
using MatchMaking.Infrastructure.Kafka.Serialization;
using Microsoft.Extensions.Logging;

namespace MatchMaking.Infrastructure.Kafka.Implementations;

public class KafkaConsumer<TKey, TValue> : IKafkaConsumer<TKey, TValue>
{
    private readonly IConsumer<TKey, TValue> _consumer;
    private readonly ILogger<KafkaConsumer<TKey, TValue>> _logger;
    private bool _disposed;

    public KafkaConsumer(
        ConsumerConfig config,
        ILogger<KafkaConsumer<TKey, TValue>> logger)
    {
        _logger = logger;

        var builder = new ConsumerBuilder<TKey, TValue>(config);

        if (typeof(TValue) != typeof(string) && typeof(TValue) != typeof(byte[]))
        {
            builder.SetValueDeserializer(new JsonDeserializer<TValue>());
        }

        if (typeof(TKey) != typeof(string) && typeof(TKey) != typeof(byte[]))
        {
            builder.SetKeyDeserializer(new JsonDeserializer<TKey>());
        }

        _consumer = builder.Build();

        _logger.LogInformation(
            "KafkaConsumer initialized for {KeyType}/{ValueType} with group: {GroupId}",
            typeof(TKey).Name,
            typeof(TValue).Name,
            config.GroupId);
    }
    public void Subscribe(string topic)
    {
        ThrowIfDisposed();
        _consumer.Subscribe(topic);
        _logger.LogInformation("Subscribed to topic: {Topic}", topic);
    }

    public void Subscribe(IEnumerable<string> topics)
    {
        ThrowIfDisposed();
        _consumer.Subscribe(topics);
        _logger.LogInformation("Subscribed to topics: {Topics}", string.Join(", ", topics));
    }

    public async Task StartConsumingAsync(
        Func<ConsumeResult<TKey, TValue>, CancellationToken, Task<bool>> messageHandler,
        CancellationToken cancellationToken)
    {
        ThrowIfDisposed();

        _logger.LogInformation("Starting Kafka consumer loop");

        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                await ProcessMessageAsync(messageHandler, cancellationToken);
            }
        }
        catch (OperationCanceledException)
        {
            _logger.LogInformation("Kafka consumer stopped via cancellation");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Fatal error in Kafka consumer loop");
            throw;
        }
    }

    private async Task ProcessMessageAsync(
        Func<ConsumeResult<TKey, TValue>, CancellationToken, Task<bool>> messageHandler,
        CancellationToken cancellationToken)
    {
        try
        {
            var consumeResult = _consumer.Consume(cancellationToken);

            if (consumeResult?.Message == null)
            {
                return;
            }

            _logger.LogDebug(
                "Consumed message from topic {Topic} at offset {Offset}",
                consumeResult.Topic,
                consumeResult.Offset);

            var success = await messageHandler(consumeResult, cancellationToken);

            if (success)
            {
                _consumer.Commit(consumeResult);
                _logger.LogDebug("Committed offset {Offset} for topic {Topic}", consumeResult.Offset, consumeResult.Topic);
            }
            else
            {
                _logger.LogWarning(
                    "Message handler returned false for offset {Offset} on topic {Topic}. Message not committed.",
                    consumeResult.Offset,
                    consumeResult.Topic);
            }
        }
        catch (ConsumeException ex)
        {
            _logger.LogError(ex, "Error consuming message from Kafka: {ErrorReason}", ex.Error.Reason);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Unexpected error processing message");
        }
    }

    public void Close()
    {
        ThrowIfDisposed();
        _consumer.Close();
        _logger.LogInformation("Kafka consumer closed");
    }

    private void ThrowIfDisposed()
    {
        if (_disposed)
        {
            throw new ObjectDisposedException(nameof(KafkaConsumer<TKey, TValue>));
        }
    }

    public void Dispose()
    {
        if (_disposed)
            return;

        try
        {
            _consumer?.Close();
            _consumer?.Dispose();
            _logger.LogInformation("KafkaConsumer disposed");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error disposing KafkaConsumer");
        }
        finally
        {
            _disposed = true;
        }
    }
}
