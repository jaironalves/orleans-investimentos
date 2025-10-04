using Orleans.Configuration;
using Orleans.Providers.Streams.Common;
using Orleans.Streams;
using StackExchange.Redis;

namespace Orleans.Investimentos.Silo.Streaming.Redis
{
    public class RedisAdapterFactory : IRedisAdapterFactory
    {
        private readonly IRedisServiceProvider provider;
        private readonly ILoggerFactory loggerFactory;
        private readonly IStreamFailureHandler streamFailureHandler;
        private readonly IStreamQueueMapper streamQueueMapper;        
                
        public static IQueueAdapterFactory Create(IServiceProvider provider, string providerName)
        {
            var factory = provider.GetRequiredKeyedService<IRedisAdapterFactory>(providerName);
            return factory;
        }

        public static IServiceCollection AddKeyedServices(IServiceCollection services, string providerName)
        {
            services
                .AddKeyedSingleton<IRedisServiceProvider>(providerName, (sp, serviceKey) =>
                {
                    var providerNameKey = $"{serviceKey}";
                    return new RedisServiceProvider(sp, providerNameKey);
                })
                .AddKeyedSingleton<IRedisAdapterFactory>(providerName, (sp, serviceKey) =>
                {
                    var providerNameKey = $"{serviceKey}";
                    var provider = sp.GetRequiredKeyedService<IRedisServiceProvider>(providerNameKey);
                    return new RedisAdapterFactory(provider);
                });

            return services;
        }

        private RedisAdapterFactory(IRedisServiceProvider provider)
        {
            this.provider = provider;

            loggerFactory = provider.GetRequiredService<ILoggerFactory>();
            streamFailureHandler = new RedisStreamFailureHandler(loggerFactory.CreateLogger<RedisStreamFailureHandler>());

            var hashRingStreamQueueMapperOptions = provider.GetOptions<HashRingStreamQueueMapperOptions>();
            streamQueueMapper = new HashRingBasedStreamQueueMapper(hashRingStreamQueueMapperOptions, provider.Name);
        }

        public Task<IQueueAdapter> CreateAdapter()
        {
            var queueAdapter = new RedisQueueAdapter(provider.Name,
                provider.GetRequiredService<IConnectionMultiplexer>(),
                streamQueueMapper, loggerFactory);

            return Task.FromResult<IQueueAdapter>(queueAdapter);
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
