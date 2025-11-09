using Microsoft.Extensions.Options;
using Orleans.Configuration;
using Orleans.Providers.Streams.Common;
using Orleans.Serialization;
using Orleans.Streams;

namespace Orleans.Investimentos.Silo.Streaming.Redis;

public class RedisStreamAdapterFactory : IQueueAdapterFactory
{
    private readonly RedisStreamServiceProvider provider;
    private readonly RedisStreamOptions options;
    private readonly Serializer<RedisStreamBatchContainer> serializer;
    private readonly IStreamFailureHandler streamFailureHandler;
    private readonly IStreamQueueMapper streamQueueMapper;
    private readonly ILoggerFactory loggerFactory;

    public static IQueueAdapterFactory Create(IServiceProvider provider, string providerName)
    {
        var factory = provider.GetRequiredKeyedService<RedisStreamAdapterFactory>(providerName);
        return factory;
    }

    public static IServiceCollection AddKeyedServices(IServiceCollection services, string providerName)
    {
        services
            .AddKeyedSingleton(providerName, (sp, serviceKey) =>
            {
                var providerNameKey = $"{serviceKey}";
                return new RedisStreamServiceProvider(sp, providerNameKey);
            })
            .AddKeyedSingleton(providerName, (sp, serviceKey) =>
            {
                var providerNameKey = $"{serviceKey}";
                var provider = sp.GetRequiredKeyedService<RedisStreamServiceProvider>(providerNameKey);
                return new RedisStreamAdapterFactory(provider);
            });

        return services;
    }

    private RedisStreamAdapterFactory(RedisStreamServiceProvider provider)
    {
        this.provider = provider;

        options = provider.GetOptions<RedisStreamOptions>();

        var providerSerializer = provider.GetRequiredService<Serializer>();
        serializer = providerSerializer.GetSerializer<RedisStreamBatchContainer>();

        loggerFactory = provider.GetRequiredService<ILoggerFactory>();
        streamFailureHandler = new RedisStreamFailureHandler(loggerFactory.CreateLogger<RedisStreamFailureHandler>());

        var hashRingStreamQueueMapperOptions = provider.GetOptions<HashRingStreamQueueMapperOptions>();
        streamQueueMapper = new HashRingBasedStreamQueueMapper(hashRingStreamQueueMapperOptions, provider.Name);
    }

    public async Task<IQueueAdapter> CreateAdapter()
    {
        var connectionMultiplexer = await options.CreateMultiplexer(provider, options);
        var clusterOptions = provider.GetRequiredService<IOptions<ClusterOptions>>().Value;

        var queueAdapter = new RedisStreamAdapter(provider, options, clusterOptions,
            serializer, connectionMultiplexer, streamQueueMapper, loggerFactory);

        return queueAdapter;
    }

    public Task<IStreamFailureHandler> GetDeliveryFailureHandler(QueueId queueId)
    {
        return Task.FromResult(streamFailureHandler);
    }

    public IQueueAdapterCache GetQueueAdapterCache()
    {
        var simpleQueueCacheOptions = provider.GetOptions<SimpleQueueCacheOptions>();
        return new SimpleQueueAdapterCache(simpleQueueCacheOptions, provider.Name, loggerFactory);
    }

    public IStreamQueueMapper GetStreamQueueMapper()
    {
        return streamQueueMapper;
    }
}
