using Microsoft.Extensions.Logging;
using Orleans.Configuration;
using Orleans.Investimentos.Streaming.Redis.Storage;
using Orleans.Runtime;
using Orleans.Streams;
using StackExchange.Redis;
using System.Collections.Concurrent;

namespace Orleans.Investimentos.Streaming.Redis;

public class RedisStreamAdapter : IQueueAdapter
{
    private readonly string providerName;
    private readonly RedisStreamOptions redisStreamOptions;
    private readonly RedisStreamReceiverOptions redisStreamReceiverOptions;

    private readonly ClusterOptions clusterOptions;
    private readonly IQueueDataAdapter<StreamEntry, IBatchContainer> dataAdapter;
    private readonly IConnectionMultiplexer connectionMultiplexer;
    private readonly IStreamQueueMapper streamQueueMapper;
    private readonly ILoggerFactory loggerFactory;

    private readonly ConcurrentDictionary<QueueId, RedisStreamStorage> StreamStorages = new();

    internal RedisStreamAdapter(string providerName,
        ClusterOptions clusterOptions,
        RedisStreamOptions redisStreamOptions,
        RedisStreamReceiverOptions redisStreamReceiverOptions,        
        IQueueDataAdapter<StreamEntry, IBatchContainer> dataAdapter,
        IConnectionMultiplexer connectionMultiplexer,
        IStreamQueueMapper streamQueueMapper,
        ILoggerFactory loggerFactory)
    {
        this.providerName = providerName;
        this.clusterOptions = clusterOptions;
        this.redisStreamOptions = redisStreamOptions;
        this.redisStreamReceiverOptions = redisStreamReceiverOptions;
        
        this.dataAdapter = dataAdapter;
        this.connectionMultiplexer = connectionMultiplexer;
        this.streamQueueMapper = streamQueueMapper;
        this.loggerFactory = loggerFactory;
    }

    public string Name => providerName;

    public bool IsRewindable => false;

    public StreamProviderDirection Direction => StreamProviderDirection.ReadWrite;

    public IQueueAdapterReceiver CreateReceiver(QueueId queueId)
    {
        var storage = GetStorage(queueId);
        return RedisStreamAdapterReceiver.Create(redisStreamOptions, dataAdapter, storage, queueId, TimeProvider.System, loggerFactory);
    }

    private RedisStreamStorage GetStorage(QueueId queueId)
    {
        var streamKey = redisStreamOptions.GetRedisKey(clusterOptions, queueId);
        var storage = new RedisStreamStorage(connectionMultiplexer, streamKey, queueId.ToString(), loggerFactory);
        return storage;
    }

    public async Task QueueMessageBatchAsync<T>(StreamId streamId, IEnumerable<T> events, StreamSequenceToken token, Dictionary<string, object> requestContext)
    {
        var queueId = streamQueueMapper.GetQueueForStream(streamId);

        if (!StreamStorages.TryGetValue(queueId, out RedisStreamStorage streamStorage))
        {
            var tmpStreamStorage = GetStorage(queueId);
            await tmpStreamStorage.InitAsync();
            streamStorage = StreamStorages.GetOrAdd(queueId, tmpStreamStorage);
        }

        var streamEntry = dataAdapter
            .ToQueueMessage(streamId, events, token, requestContext);

        await streamStorage
            .AddEntryAsync(streamEntry);
    }
}
