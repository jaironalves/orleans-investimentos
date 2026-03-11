using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using Orleans.Configuration;
using Orleans.Configuration.Overrides;
using Orleans.Providers.Streams.Common;
using Orleans.Streams;
using StackExchange.Redis;

namespace Orleans.Investimentos.Streaming.Redis;

public class RedisStreamAdapterFactory : IQueueAdapterFactory
{
    //private readonly RedisStreamServiceProvider provider;
    private readonly string _providerName;
    private readonly ClusterOptions _clusterOptions;
    private readonly RedisStreamOptions _redisStreamOptions;
    private readonly RedisStreamReceiverOptions _redisStreamReceiverOptions;
    private readonly IQueueDataAdapter<StreamEntry, IBatchContainer> _queueDataAdapter;
    private readonly IStreamFailureHandler _streamFailureHandler;
    private readonly IStreamQueueMapper _streamQueueMapper;
    private readonly IQueueAdapterCache _queueAdapterCache;
    private readonly ILoggerFactory _loggerFactory;

    //public static IQueueAdapterFactory Create(IServiceProvider serviceProvider, string providerName)
    //{
    //    var redisStreamServiceProvider = new RedisStreamServiceProvider(serviceProvider, providerName);
    //    var redisStreamAdapterFactory = new RedisStreamAdapterFactory(redisStreamServiceProvider);
    //    return redisStreamAdapterFactory;
    //}

    public static IQueueAdapterFactory Create(IServiceProvider serviceProvider, string providerName)
    {
        var clusterOptions = serviceProvider.GetProviderClusterOptions(providerName).Value;
        var redisStreamOptions = serviceProvider.GetOptionsByName<RedisStreamOptions>(providerName);
        var redisStreamReceiverOptions = serviceProvider.GetOptionsByName<RedisStreamReceiverOptions>(providerName);
        var hashRingStreamQueueMapperOptions = serviceProvider.GetOptionsByName<HashRingStreamQueueMapperOptions>(providerName);
        var simpleQueueCacheOptions = serviceProvider.GetOptionsByName<SimpleQueueCacheOptions>(providerName);
        var queueDataAdapter = serviceProvider.GetRequiredKeyedService<IQueueDataAdapter<StreamEntry, IBatchContainer>>(providerName);

        //var receiverOptions = serviceProvider.GetOptionsByName<RedisStreamReceiverOptions>(name);

        return ActivatorUtilities
            .CreateInstance<RedisStreamAdapterFactory>(serviceProvider,
                providerName, clusterOptions, redisStreamOptions, redisStreamReceiverOptions,
                hashRingStreamQueueMapperOptions, simpleQueueCacheOptions,
                queueDataAdapter);
    }

    public static IServiceCollection PostConfigureDefaults(IServiceCollection services, string providerName)
    {
        services.
            TryAddKeyedSingleton<IQueueDataAdapter<StreamEntry, IBatchContainer>, RedisStreamDataAdapter>(providerName);

        return services;
    }

    //private RedisStreamAdapterFactory(RedisStreamServiceProvider provider)
    //{
    //    this.provider = provider;

    //    _redisStreamOptions = provider.GetOptions<RedisStreamOptions>();

    //    _queueDataAdapter = provider.GetComponentService<IQueueDataAdapter<StreamEntry, IBatchContainer>>();

    //    _loggerFactory = provider.GetRequiredService<ILoggerFactory>();
    //    _streamFailureHandler = new RedisStreamFailureHandler(_loggerFactory.CreateLogger<RedisStreamFailureHandler>());

    //    var hashRingStreamQueueMapperOptions = provider.GetOptions<HashRingStreamQueueMapperOptions>();
    //    _streamQueueMapper = new HashRingBasedStreamQueueMapper(hashRingStreamQueueMapperOptions, provider.Name);
    //}

    internal RedisStreamAdapterFactory() { }

    public RedisStreamAdapterFactory(
        string providerName,
        ClusterOptions clusterOptions,
        RedisStreamOptions redisStreamOptions,
        RedisStreamReceiverOptions redisStreamReceiverOptions,
        HashRingStreamQueueMapperOptions hashRingStreamQueueMapperOptions,
        SimpleQueueCacheOptions simpleQueueCacheOptions,
        IQueueDataAdapter<StreamEntry, IBatchContainer> queueDataAdapter,
        ILoggerFactory loggerFactory)
    {
        _providerName = providerName;
        _clusterOptions = clusterOptions;

        _redisStreamOptions = redisStreamOptions;
        _redisStreamReceiverOptions = redisStreamReceiverOptions;
        _queueDataAdapter = queueDataAdapter;
        _loggerFactory = loggerFactory;

        _streamQueueMapper = new HashRingBasedStreamQueueMapper(hashRingStreamQueueMapperOptions, providerName);
        _queueAdapterCache = new SimpleQueueAdapterCache(simpleQueueCacheOptions, providerName, loggerFactory);
        _streamFailureHandler = new RedisStreamFailureHandler(loggerFactory.CreateLogger<RedisStreamFailureHandler>());
    }

    public async Task<IQueueAdapter> CreateAdapter()
    {
        var connectionMultiplexer = await _redisStreamOptions.CreateMultiplexer(_redisStreamOptions);

        var queueAdapter = new RedisStreamAdapter(_providerName, _clusterOptions, _redisStreamOptions,
            _redisStreamReceiverOptions, _queueDataAdapter, connectionMultiplexer, _streamQueueMapper,
            _loggerFactory);

        return queueAdapter;
    }

    public Task<IStreamFailureHandler> GetDeliveryFailureHandler(QueueId queueId)
    {
        return Task.FromResult(_streamFailureHandler);
    }

    public IQueueAdapterCache GetQueueAdapterCache()
    {
        return _queueAdapterCache;
    }

    public IStreamQueueMapper GetStreamQueueMapper()
    {
        return _streamQueueMapper;
    }
}
