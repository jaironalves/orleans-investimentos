using Orleans.Investimentos.Silo.Streaming.Redis.Storage;
using Orleans.Streams;
using StackExchange.Redis;
using System.Collections.Concurrent;

namespace Orleans.Investimentos.Silo.Streaming.Redis
{
    public class RedisQueueAdapter : IQueueAdapter
    {
        private readonly string providerName;
        private readonly IConnectionMultiplexer connectionMultiplexer;
        private readonly IStreamQueueMapper streamQueueMapper;
        private readonly ILoggerFactory loggerFactory;

        protected readonly ConcurrentDictionary<QueueId, IRedisStreamStorage> StreamStorages = new();

        internal RedisQueueAdapter(string providerName,
            IConnectionMultiplexer connectionMultiplexer,
            IStreamQueueMapper streamQueueMapper,
            ILoggerFactory loggerFactory)
        {
            this.providerName = providerName;
            this.connectionMultiplexer = connectionMultiplexer;
            this.streamQueueMapper = streamQueueMapper;
            this.loggerFactory = loggerFactory;
        }

        public string Name => providerName;

        public bool IsRewindable => false;

        public StreamProviderDirection Direction => StreamProviderDirection.ReadWrite;

        public IQueueAdapterReceiver CreateReceiver(QueueId queueId)
        {
            return RedisQueueAdapterReceiver.Create(connectionMultiplexer, queueId, TimeProvider.System, loggerFactory);            
        }

        public async Task QueueMessageBatchAsync<T>(StreamId streamId, IEnumerable<T> events, StreamSequenceToken token, Dictionary<string, object> requestContext)
        {
            var queueId = streamQueueMapper.GetQueueForStream(streamId);

            if (!StreamStorages.TryGetValue(queueId, out IRedisStreamStorage? streamStorage))
            {
                var tmpStreamStorage = new RedisStreamStorage(connectionMultiplexer, queueId.ToString(), loggerFactory);
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
