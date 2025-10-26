using Microsoft.Extensions.Options;
using Orleans.Configuration;
using Orleans.Investimentos.Silo.Streaming.Redis.Storage;
using Orleans.Streams;
using StackExchange.Redis;
using System.Collections.Concurrent;

namespace Orleans.Investimentos.Silo.Streaming.Redis
{
    public class RedisAdapter : IQueueAdapter
    {
        private readonly RedisServiceProvider provider;
        private readonly RedisStreamOptions options;

        private readonly ClusterOptions clusterOptions;
        private readonly IConnectionMultiplexer connectionMultiplexer;        
        private readonly IStreamQueueMapper streamQueueMapper;
        private readonly ILoggerFactory loggerFactory;

        private readonly ConcurrentDictionary<QueueId, RedisStorage> StreamStorages = new();

        internal RedisAdapter(RedisServiceProvider provider,            
            RedisStreamOptions options,                        
            ClusterOptions clusterOptions,
            IConnectionMultiplexer connectionMultiplexer,
            IStreamQueueMapper streamQueueMapper,
            ILoggerFactory loggerFactory)
        {
            this.provider = provider;
            this.options = options;
            this.clusterOptions = clusterOptions;
            this.connectionMultiplexer = connectionMultiplexer;
            this.streamQueueMapper = streamQueueMapper;
            this.loggerFactory = loggerFactory;            
        }

        public string Name => provider.Name;

        public bool IsRewindable => false;

        public StreamProviderDirection Direction => StreamProviderDirection.ReadWrite;

        public IQueueAdapterReceiver CreateReceiver(QueueId queueId)
        {
            var storage = GetStorage(queueId);
            return RedisAdapterReceiver.Create(storage, queueId, TimeProvider.System, loggerFactory);
        }

        private RedisStorage GetStorage(QueueId queueId)
        {
            var streamKey = options.GetRedisKey(clusterOptions, queueId);
            var storage = new RedisStorage(connectionMultiplexer, streamKey, queueId.ToString(), loggerFactory);
            return storage;
        }

        public async Task QueueMessageBatchAsync<T>(StreamId streamId, IEnumerable<T> events, StreamSequenceToken token, Dictionary<string, object> requestContext)
        {
            var queueId = streamQueueMapper.GetQueueForStream(streamId);

            if (!StreamStorages.TryGetValue(queueId, out RedisStorage? streamStorage))
            {
                var tmpStreamStorage = GetStorage(queueId);
                await tmpStreamStorage.InitAsync();
                streamStorage = StreamStorages.GetOrAdd(queueId, tmpStreamStorage);
            }

            var entries = RedisBatchContainer
                .ToStreamEntries(streamId, events);

            await streamStorage
                .AddEntriesAsync(entries);
        }
    }
}
