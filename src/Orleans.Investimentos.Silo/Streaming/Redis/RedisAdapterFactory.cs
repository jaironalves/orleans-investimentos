using Microsoft.Extensions.Options;
using Orleans.Configuration;
using Orleans.Providers.Streams.Common;
using Orleans.Streams;
using StackExchange.Redis;

namespace Orleans.Investimentos.Silo.Streaming.Redis
{
    public class RedisAdapterFactory : IQueueAdapterFactory
    {
        private readonly RedisServiceProvider provider;
        private readonly RedisStreamOptions options;
        private readonly IStreamFailureHandler streamFailureHandler;
        private readonly IStreamQueueMapper streamQueueMapper;
        private readonly ILoggerFactory loggerFactory;

        public static IQueueAdapterFactory Create(IServiceProvider provider, string providerName)
        {
            var factory = provider.GetRequiredKeyedService<RedisAdapterFactory>(providerName);
            return factory;
        }

        public static IServiceCollection AddKeyedServices(IServiceCollection services, string providerName)
        {
            services
                .AddKeyedSingleton<RedisServiceProvider>(providerName, (sp, serviceKey) =>
                {
                    var providerNameKey = $"{serviceKey}";
                    return new RedisServiceProvider(sp, providerNameKey);
                })
                .AddKeyedSingleton<RedisAdapterFactory>(providerName, (sp, serviceKey) =>
                {
                    var providerNameKey = $"{serviceKey}";
                    var provider = sp.GetRequiredKeyedService<RedisServiceProvider>(providerNameKey);
                    return new RedisAdapterFactory(provider);
                });

            return services;
        }

        private RedisAdapterFactory(RedisServiceProvider provider)
        {
            this.provider = provider;

            options = provider.GetOptions<RedisStreamOptions>();

            loggerFactory = provider.GetRequiredService<ILoggerFactory>();
            streamFailureHandler = new RedisStreamFailureHandler(loggerFactory.CreateLogger<RedisStreamFailureHandler>());

            var hashRingStreamQueueMapperOptions = provider.GetOptions<HashRingStreamQueueMapperOptions>();
            streamQueueMapper = new HashRingBasedStreamQueueMapper(hashRingStreamQueueMapperOptions, provider.Name);
        }

        public async Task<IQueueAdapter> CreateAdapter()
        {
            var connectionMultiplexer = await options.CreateMultiplexer(provider, options);
            var clusterOptions = provider.GetRequiredService<IOptions<ClusterOptions>>().Value;

            var queueAdapter = new RedisAdapter(provider, options, clusterOptions,
                connectionMultiplexer, streamQueueMapper, loggerFactory);

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
}
