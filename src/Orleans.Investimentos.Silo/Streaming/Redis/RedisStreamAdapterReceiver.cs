using Orleans.Investimentos.Silo.Streaming.Redis.Storage;
using Orleans.Serialization;
using Orleans.Streams;
using StackExchange.Redis;

namespace Orleans.Investimentos.Silo.Streaming.Redis;

internal partial class RedisStreamAdapterReceiver : IQueueAdapterReceiver
{
    private readonly RedisStreamOptions options;
    private readonly Serializer<RedisStreamBatchContainer> serializer;
    private RedisStreamStorage streamStorage;
    private readonly QueueId queueId;
    private readonly TimeProvider timeProvider;
    private readonly ILogger<RedisStreamAdapterReceiver> logger;

    private Task outstandingTask;
    private string lastId = "$";    

    private DateTimeOffset lastTrimTime;

    internal static IQueueAdapterReceiver Create(RedisStreamOptions options,
        Serializer<RedisStreamBatchContainer> serializer, RedisStreamStorage storage,
        QueueId queueId, TimeProvider timeProvider, ILoggerFactory loggerFactory)
    {
        if (queueId.IsDefault) throw new ArgumentNullException(nameof(queueId));
        ArgumentNullException.ThrowIfNull(timeProvider);
        ArgumentNullException.ThrowIfNull(loggerFactory);

        return new RedisStreamAdapterReceiver(options, serializer, storage, queueId, timeProvider, loggerFactory.CreateLogger<RedisStreamAdapterReceiver>());
    }

    private RedisStreamAdapterReceiver(
        RedisStreamOptions options,
        Serializer<RedisStreamBatchContainer> serializer,
        RedisStreamStorage streamStorage,        
        QueueId queueId, TimeProvider timeProvider,
        ILogger<RedisStreamAdapterReceiver> logger)
    {
        this.options = options;
        this.serializer = serializer;
        this.streamStorage = streamStorage;
        this.queueId = queueId;
        this.timeProvider = timeProvider;
        this.logger = logger;

        lastTrimTime = timeProvider.GetUtcNow();
    }

    public Task Initialize(TimeSpan timeout)
    {
        if (streamStorage != null) // check in case we already shut it down.
        {
            return streamStorage.InitAsync();
        }
        return Task.CompletedTask;
    }

    public async Task Shutdown(TimeSpan timeout)
    {
        try
        {
            // await the last storage operation, so after we shutdown and stop this receiver we don't get async operation completions from pending storage operations.
            if (outstandingTask != null)
                await outstandingTask;
        }
        finally
        {
            // remember that we shut down so we never try to read from the queue again.
            streamStorage = null;
        }
    }

    public async Task<IList<IBatchContainer>?> GetQueueMessagesAsync(int maxCount)
    {
        try
        {
            var streamStorageRef = streamStorage; // store direct ref, in case we are somehow asked to shutdown while we are receiving.
            if (streamStorageRef == null)
                return [];

            var task = streamStorageRef
                .GetEntriesAsync(lastId, maxCount);

            outstandingTask = task;
            lastId = ">";

            var streamMessages = await task;

            var messageBatch = streamMessages
                .Select(streamEntry => RedisStreamBatchContainer.FromStreamEntry(streamEntry, serializer))
                .ToList<IBatchContainer>();

            return messageBatch;
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Error reading from stream {QueueId}", queueId);
            return default;
        }
        finally
        {
            outstandingTask = null;

            await TrimStorageAsyncIfNeeded();
        }
    }

    private async Task TrimStorageAsyncIfNeeded()
    {
        try
        {
            if (timeProvider.GetUtcNow() - lastTrimTime < TimeSpan.FromMinutes(options.TrimTimeMinutes))
                return;

            var streamStorageRef = streamStorage; // store direct ref, in case we are somehow asked to shutdown while we are receiving.
            if (streamStorageRef == null)
                return;

            outstandingTask = streamStorageRef.TrimAsync(options.MaxStreamLength, true);
            try
            {
                await outstandingTask;
                lastTrimTime = timeProvider.GetUtcNow();
            }
            catch (Exception exc)
            {
                LogWarningOperationException(logger, exc, nameof(streamStorageRef.EntryDeliveredAsync), queueId);
            }
        }
        finally
        {
            outstandingTask = null;
        }
    }

    public async Task MessagesDeliveredAsync(IList<IBatchContainer> messages)
    {
        try
        {
            var streamStorageRef = streamStorage; // store direct ref, in case we are somehow asked to shutdown while we are receiving.            
            if (messages.Count == 0 || streamStorageRef == null)
                return;

            List<RedisValue> streamEntryMessages = [.. messages.Cast<RedisStreamBatchContainer>().Select(b => b.StreamEntryId)];
            outstandingTask = Task.WhenAll(streamEntryMessages.Select(streamStorageRef.EntryDeliveredAsync));
            try
            {
                await outstandingTask;
            }
            catch (Exception exc)
            {
                LogWarningOperationException(logger, exc, nameof(streamStorageRef.EntryDeliveredAsync), queueId);
            }
        }
        finally
        {
            outstandingTask = null;
        }
    }

    [LoggerMessage(
        Level = LogLevel.Warning,
        Message = "Exception upon {Operation} on queue {QueueId}. Ignoring."
    )]
    private partial void LogWarningOperationException(ILogger logger, Exception exception, string operation, QueueId queueId);
}
