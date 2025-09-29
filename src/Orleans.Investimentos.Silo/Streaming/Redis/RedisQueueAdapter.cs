using Microsoft.Extensions.Options;
using Orleans.Configuration;
using Orleans.Investimentos.Silo.Streaming.Redis.Storage;
using Orleans.Streams;
using StackExchange.Redis;
using System.Text.Json;

namespace Orleans.Investimentos.Silo.Streaming.Redis
{
    public class RedisQueueAdapter : IQueueAdapter
    {
        private readonly string providerName;
        private readonly IRedisStreamStorage streamStorage;
        private readonly IStreamQueueMapper streamQueueMapper;
        private readonly ILoggerFactory loggerFactory;

        //private readonly IDatabase _database;
        
        //private readonly HashRingBasedStreamQueueMapper _hashRingBasedStreamQueueMapper;
        //private readonly ILoggerFactory _loggerFactory;
        //private readonly ILogger<RedisQueueAdapter> _logger;
        //private readonly IOptions<RedisQueueAdapterReceiverOptions> _receiverOptions; // Added receiver options

        // Changed: Constructor to accept IOptions<RedisStreamReceiverOptions>
        //public RedisQueueAdapter(IDatabase database,
        //                        string providerName,
        //                        HashRingBasedStreamQueueMapper hashRingBasedStreamQueueMapper,
        //                        ILoggerFactory loggerFactory,
        //                        IOptions<RedisQueueAdapterReceiverOptions> receiverOptions)
        //{
        //    _database = database ?? throw new ArgumentNullException(nameof(database));
        //    _providerName = providerName ?? throw new ArgumentNullException(nameof(providerName));
        //    _hashRingBasedStreamQueueMapper = hashRingBasedStreamQueueMapper ?? throw new ArgumentNullException(nameof(hashRingBasedStreamQueueMapper));
        //    _loggerFactory = loggerFactory ?? throw new ArgumentNullException(nameof(loggerFactory));
        //    _logger = loggerFactory.CreateLogger<RedisQueueAdapter>();
        //    _receiverOptions = receiverOptions ?? throw new ArgumentNullException(nameof(receiverOptions)); // Store receiver options


        //    streamQueueMapper = _hashRingBasedStreamQueueMapper;            
        //}

        public RedisQueueAdapter(string providerName,
            IRedisStreamStorage streamStorage,
            IStreamQueueMapper streamQueueMapper,
            ILoggerFactory loggerFactory)
        {
            this.providerName = providerName;
            this.streamStorage = streamStorage;
            this.streamQueueMapper = streamQueueMapper;
            this.loggerFactory = loggerFactory;
        }

        public string Name => providerName;

        public bool IsRewindable => false;

        public StreamProviderDirection Direction => StreamProviderDirection.ReadWrite;

        public IQueueAdapterReceiver CreateReceiver(QueueId queueId)
        {
            return default;
            //return new RedisQueueAdapterReceiver(queueId, _database, _loggerFactory.CreateLogger<RedisQueueAdapterReceiver>(), TimeProvider.System, _receiverOptions);
        }

        public async Task QueueMessageBatchAsync<T>(StreamId streamId, IEnumerable<T> events, StreamSequenceToken token, Dictionary<string, object> requestContext)
        {
            var queueId = streamQueueMapper.GetQueueForStream(streamId);

            var entries = RedisBatchContainer
                .ToStreamEntries(streamId, events);

            await streamStorage.AddEntriesAsync(queueId.ToString(), entries);

            //try
            //{


            //    //foreach (var @event in events)
            //    //{
            //    //    NameValueEntry streamNamespaceEntry = new("streamNamespace", streamId.Namespace);
            //    //    NameValueEntry streamKeyEntry = new("streamKey", streamId.Key);
            //    //    NameValueEntry eventTypeEntry = new("eventType", @event!.GetType().Name);
            //    //    NameValueEntry dataEntry = new("data", JsonSerializer.Serialize(@event));                    
            //    //    await _database.StreamAddAsync(queueId.ToString(), [streamNamespaceEntry, streamKeyEntry, eventTypeEntry, dataEntry]);
            //    //}
            //}
            //catch (Exception ex)
            //{
            //    _logger.LogError(ex, "Error adding event to stream {StreamId}", streamId);
            //}
        }
    }
}
