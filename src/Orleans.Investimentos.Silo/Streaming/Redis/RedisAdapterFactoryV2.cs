using Microsoft.Extensions.Options;
using Orleans.Configuration;
using Orleans.Providers.Streams.Common;
using Orleans.Streams;
using StackExchange.Redis;

namespace Orleans.Investimentos.Silo.Streaming.Redis
{
    public class RedisAdapterFactoryV2 : IRedisAdapterFactory
    {
        private readonly IConnectionMultiplexer _connectionMultiplexer;
        private readonly ILoggerFactory _loggerFactory;
        private readonly IRedisProviderName _redisProviderName;
        private readonly IStreamFailureHandler _streamFailureHandler;
        private readonly SimpleQueueCacheOptions _simpleQueueCacheOptions;
        private readonly HashRingBasedStreamQueueMapper _hashRingBasedStreamQueueMapper;
        private readonly RedisQueueAdapterReceiverOptions _receiverOptions;

        public RedisAdapterFactoryV2(
            IServiceProvider serviceProvider,
            IRedisProviderName redisProviderName)
        {
            _redisProviderName = redisProviderName;
            var providerName = _redisProviderName.Name ?? throw new ArgumentNullException(nameof(redisProviderName));

            _connectionMultiplexer = serviceProvider.GetRequiredService<IConnectionMultiplexer>();
            _loggerFactory = serviceProvider.GetRequiredService<ILoggerFactory>();

            _simpleQueueCacheOptions = serviceProvider.GetOptionsByName<SimpleQueueCacheOptions>(providerName);


            var hashRingStreamQueueMapperOptions = serviceProvider.GetOptionsByName<HashRingStreamQueueMapperOptions>(providerName);
            _hashRingBasedStreamQueueMapper = new HashRingBasedStreamQueueMapper(hashRingStreamQueueMapperOptions, providerName);

            
            _receiverOptions = serviceProvider.GetOptionsByName<RedisQueueAdapterReceiverOptions>(providerName);
                        
            _streamFailureHandler = new RedisStreamFailureHandler(_loggerFactory.CreateLogger<RedisStreamFailureHandler>());
        }

        public static IQueueAdapterFactory Create(IServiceProvider provider, string providerName)
        {
            var factory = provider.GetRequiredKeyedService<IRedisAdapterFactory>(providerName);
            return factory;
        }

        public Task<IQueueAdapter> CreateAdapter()
        {
            // Pass receiver options to RedisStreamAdapter
            return Task.FromResult<IQueueAdapter>(new RedisQueueAdapter(_connectionMultiplexer.GetDatabase(), _redisProviderName.Name, _hashRingBasedStreamQueueMapper, _loggerFactory, Options.Create(_receiverOptions)));
        }

        public Task<IStreamFailureHandler> GetDeliveryFailureHandler(QueueId queueId)
        {
            return Task.FromResult(_streamFailureHandler);
        }

        public IQueueAdapterCache GetQueueAdapterCache()
        {
            return new SimpleQueueAdapterCache(_simpleQueueCacheOptions, _redisProviderName.Name, _loggerFactory);
        }

        public IStreamQueueMapper GetStreamQueueMapper()
        {
            return _hashRingBasedStreamQueueMapper;
        }
    }
}
