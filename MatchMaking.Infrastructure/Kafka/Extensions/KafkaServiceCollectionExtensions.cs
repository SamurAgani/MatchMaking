using Confluent.Kafka;
using MatchMaking.Infrastructure.Kafka.Abstractions;
using MatchMaking.Infrastructure.Kafka.Implementations;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace MatchMaking.Infrastructure.Kafka.Extensions;

public static class KafkaServiceCollectionExtensions
{
    public static IServiceCollection AddKafkaProducer<TKey, TValue>(
        this IServiceCollection services,
        IConfiguration configuration,
        string configSection = "Kafka:Producer")
    {
        services.AddSingleton<IKafkaProducer<TKey, TValue>>(sp =>
        {
            var logger = sp.GetRequiredService<ILogger<KafkaProducer<TKey, TValue>>>();
            var bootstrapServers = configuration["Kafka:BootstrapServers"];

            var config = new ProducerConfig
            {
                BootstrapServers = bootstrapServers,
                ClientId = configuration[$"{configSection}:ClientId"],
            };

            return new KafkaProducer<TKey, TValue>(config, logger);
        });

        return services;
    }

    public static IServiceCollection AddKafkaConsumer<TKey, TValue>(
        this IServiceCollection services,
        IConfiguration configuration,
        string configSection = "Kafka:Consumer")
    {
        services.AddSingleton<IKafkaConsumer<TKey, TValue>>(sp =>
        {
            var logger = sp.GetRequiredService<ILogger<KafkaConsumer<TKey, TValue>>>();
            var bootstrapServers = configuration["Kafka:BootstrapServers"];

            var config = new ConsumerConfig
            {
                BootstrapServers = bootstrapServers,
                GroupId = configuration[$"{configSection}:GroupId"],
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoCommit = false,
            };

            return new KafkaConsumer<TKey, TValue>(config, logger);
        });

        return services;
    }
}
