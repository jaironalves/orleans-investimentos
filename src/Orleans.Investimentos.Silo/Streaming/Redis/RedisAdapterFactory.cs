using Microsoft.Extensions.Options;
using Orleans.Configuration;
using Orleans.Investimentos.Silo.Streaming.Redis.Storage;
using Orleans.Providers.Streams.Common;
using Orleans.Streams;
using StackExchange.Redis;
using System.Xml.Linq;

namespace Orleans.Investimentos.Silo.Streaming.Redis
{
    public class RedisAdapterFactory : IRedisAdapterFactory
    {
        private readonly IRedisServiceProvider provider;
        private readonly ILoggerFactory loggerFactory;
        private readonly IStreamFailureHandler streamFailureHandler;
        private readonly IStreamQueueMapper streamQueueMapper;

        //private readonly IConnectionMultiplexer _connectionMultiplexer;
        //private readonly ILoggerFactory _loggerFactory;

        //private readonly IStreamFailureHandler _streamFailureHandler;
        //private readonly SimpleQueueCacheOptions _simpleQueueCacheOptions;
        //private readonly HashRingBasedStreamQueueMapper _hashRingBasedStreamQueueMapper;
        //private readonly RedisQueueAdapterReceiverOptions _receiverOptions;

        public RedisAdapterFactory(IRedisServiceProvider provider)
        {
            this.provider = provider;

            loggerFactory = provider.GetRequiredService<ILoggerFactory>();
            streamFailureHandler = new RedisStreamFailureHandler(loggerFactory.CreateLogger<RedisStreamFailureHandler>());

            var hashRingStreamQueueMapperOptions = provider.GetOptions<HashRingStreamQueueMapperOptions>();
            streamQueueMapper = new HashRingBasedStreamQueueMapper(hashRingStreamQueueMapperOptions, provider.Name);            
        }

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
                .AddKeyedSingleton<IRedisStreamStorage>(providerName, (sp, serviceKey) =>
                {
                    var providerNameKey = $"{serviceKey}";
                    var provider = sp.GetRequiredKeyedService<IRedisServiceProvider>(providerNameKey);
                    return new RedisStreamStorage(provider);
                })
                .AddKeyedSingleton<IRedisAdapterFactory>(providerName, (sp, serviceKey) =>
                {
                    var providerNameKey = $"{serviceKey}";
                    var provider = sp.GetRequiredKeyedService<IRedisServiceProvider>(providerNameKey);
                    return new RedisAdapterFactory(provider);
                });

            return services;
        }

        public Task<IQueueAdapter> CreateAdapter()
        {
            return Task.FromResult<IQueueAdapter>(new RedisQueueAdapter(provider));
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
