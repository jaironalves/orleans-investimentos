using Microsoft.Extensions.Logging;
using Orleans.Configuration;
using Orleans.Streams;
using StackExchange.Redis;
using System.Diagnostics.CodeAnalysis;

namespace Orleans.Investimentos.Streaming.Redis.Storage;

//internal partial class RedisStreamStorage(IConnectionMultiplexer connectionMultiplexer,
//    RedisKey streamKey, string streamName, ILoggerFactory loggerFactory)
//{
//    private const string GROUP_NAME = "orleans-redis-stream-consumer";

//    private readonly ILogger<RedisStreamStorage> logger = loggerFactory.CreateLogger<RedisStreamStorage>();

//    private readonly IDatabase database = connectionMultiplexer.GetDatabase();

//    public async Task InitAsync()
//    {
//        try
//        {
//            await database
//                .StreamCreateConsumerGroupAsync(streamKey, GROUP_NAME, "$", true);
//        }
//        catch (Exception exc) when (exc.Message.Contains("name already exists"))
//        {
//            logger.LogInformation("Consumer group {Consumer} already exists for stream {StreamName}", GROUP_NAME, streamName);
//        }
//        catch (Exception exc)
//        {
//            ReportErrorAndRethrow(exc, nameof(InitAsync));
//        }
//    }

//    public async Task<StreamEntry> AddEntryAsync(StreamEntry entry)
//    {
//        var id = RedisValue.Null;

//        try
//        {
//            id = await database.StreamAddAsync(streamKey, entry.Values);            
//        }
//        catch (Exception exc)
//        {
//            ReportErrorAndRethrow(exc, nameof(AddEntryAsync));
//        }

//        return new StreamEntry(id, entry.Values);
//    }    

//    public async Task<IEnumerable<StreamEntry>> GetEntriesAsync(RedisValue? position = null, int? count = null)
//    {
//        IEnumerable<StreamEntry> entries = [];
//        try
//        {
//            entries = await database.StreamReadGroupAsync(streamKey, GROUP_NAME, streamName, position ?? ">", count);
//        }
//        catch (Exception exc)
//        {
//            ReportErrorAndRethrow(exc, nameof(GetEntriesAsync));
//        }
//        return entries;
//    }

//    public async Task EntryAcknowledgeAsync(RedisValue messageId)
//    {
//        try
//        {
//            await database.StreamAcknowledgeAsync(streamKey, GROUP_NAME, messageId);
//        }
//        catch (Exception exc)
//        {
//            ReportErrorAndRethrow(exc, nameof(EntryAcknowledgeAsync));
//        }
//    }

//    public async Task TrimAsync(int maxLength, bool useApproximateMaxLength)
//    {
//        try
//        {
//            var trimMessagesCount = await database.StreamTrimAsync(streamKey, maxLength, useApproximateMaxLength);
//            if (trimMessagesCount > 0)
//            {
//                logger.LogInformation("Trimmed Redis stream {StreamName} to max length {MaxLength}, removed {TrimmedCount} entries", streamName, maxLength, trimMessagesCount);
//            }
//        }
//        catch (Exception exc)
//        {
//            ReportErrorAndRethrow(exc, nameof(TrimAsync));
//        }
//    }

//    [DoesNotReturn]
//    private void ReportErrorAndRethrow(Exception exc, string operation)
//    {
//        LogErrorRedisOperation(exc, operation, streamName);
//        throw new AggregateException($"Error doing {operation} for Redis stream {streamName}", exc);
//    }

//    [LoggerMessage(
//        EventId = (int)ErrorCode.StreamProviderManagerBase,
//        Level = LogLevel.Error,
//        Message = "Error doing {Operation} for Redis stream {StreamName}"
//    )]
//    private partial void LogErrorRedisOperation(Exception exception, string operation, string streamName);
//}

internal partial class RedisStreamStorage
{
    public const int MaxNumberOfMsgToGet = 1000;

    private readonly string _streamQueueIdName;
    private readonly RedisStreamOptions _redisStreamOptions;
    private readonly RedisStreamReceiverOptions _redisStreamReceiverOptions;
    private readonly ILogger<RedisStreamStorage> _logger;

    private readonly RedisKey _streamRedisKey;
    private IDatabase _database;

    public RedisStreamStorage(
        QueueId queueId,
        ClusterOptions clusterOptions,
        RedisStreamOptions redisStreamOptions,
        RedisStreamReceiverOptions redisStreamReceiverOptions,
        ILoggerFactory loggerFactory)
    {
        _streamQueueIdName = queueId.ToString();
        _redisStreamOptions = redisStreamOptions;
        _redisStreamReceiverOptions = redisStreamReceiverOptions;
        _logger = loggerFactory.CreateLogger<RedisStreamStorage>();

        _streamRedisKey = _redisStreamOptions.GetRedisKey(clusterOptions, queueId);
    }

    public async Task InitializeAsync()
    {
        await ConnectAsync();
        await CreateGroupAsync();
    }

    private async Task ConnectAsync()
    {
        try
        {
            _database ??= (await _redisStreamOptions.CreateMultiplexer.Invoke(_redisStreamOptions)).GetDatabase();
        }
        catch (Exception exc)
        {
            ReportErrorAndRethrow(exc, nameof(ConnectAsync));
        }
    }

    private async Task CreateGroupAsync()
    {
        try
        {
            await _database
                .StreamCreateConsumerGroupAsync(_streamRedisKey, _redisStreamReceiverOptions.ConsumerGroupName, position: 0, createStream: true);
        }
        catch (Exception exc) when (exc.Message.Contains("name already exists"))
        {
            _logger.LogInformation("Consumer group {GroupName} already exists for stream {StreamQueueIdName}", _redisStreamReceiverOptions.ConsumerGroupName, _streamQueueIdName);
        }
        catch (Exception exc)
        {
            ReportErrorAndRethrow(exc, nameof(CreateGroupAsync));
        }
    }

    public async Task<StreamEntry> AddEntryAsync(StreamEntry entry)
    {
        var id = RedisValue.Null;

        try
        {
            id = await _database.StreamAddAsync(_streamRedisKey, entry.Values);
        }
        catch (Exception exc)
        {
            ReportErrorAndRethrow(exc, nameof(AddEntryAsync));
        }

        return new StreamEntry(id, entry.Values);
    }

    public async Task<IEnumerable<StreamEntry>> GetEntriesAsync(int count)
    {
        IEnumerable<StreamEntry> entriesResult = [];
        try
        {
            var claimResult = await _database.StreamAutoClaimAsync(_streamRedisKey,
                _redisStreamReceiverOptions.ConsumerGroupName,
                _redisStreamReceiverOptions.ConsumerName,
                (long)_redisStreamReceiverOptions.DeliveredMessageIdleTimeout.TotalMilliseconds,
                startAtId: 0, count);

            if (claimResult.ClaimedEntries.Length == count)
            {
                entriesResult = claimResult.ClaimedEntries;
            }
            else
            {
                var entriesReadGroup = await _database.StreamReadGroupAsync(_streamRedisKey,
                    _redisStreamReceiverOptions.ConsumerGroupName,
                    _redisStreamReceiverOptions.ConsumerName, position: ">",
                    count - claimResult.ClaimedEntries.Length);

                entriesResult = claimResult.ClaimedEntries.Length != 0 ?
                    claimResult.ClaimedEntries.Concat(entriesReadGroup) : entriesReadGroup;
            }

        }
        catch (Exception exc)
        {
            ReportErrorAndRethrow(exc, nameof(GetEntriesAsync));
        }
        return entriesResult;
    }

    public async Task EntryAcknowledgeAsync(RedisValue messageId)
    {
        try
        {
            await _database.StreamAcknowledgeAsync(_streamRedisKey, 
                _redisStreamReceiverOptions.ConsumerGroupName, messageId);
        }
        catch (Exception exc)
        {
            ReportErrorAndRethrow(exc, nameof(EntryAcknowledgeAsync));
        }
    }

    public async Task TrimAsync(int maxLength, bool useApproximateMaxLength)
    {
        try
        {
            var trimMessagesCount = await _database.StreamTrimAsync(_streamRedisKey, maxLength, useApproximateMaxLength);
            if (trimMessagesCount > 0)
            {
                _logger.LogInformation("Trimmed Redis stream {StreamName} to max length {MaxLength}, removed {TrimmedCount} entries", _streamQueueIdName, maxLength, trimMessagesCount);
            }
        }
        catch (Exception exc)
        {
            ReportErrorAndRethrow(exc, nameof(TrimAsync));
        }
    }

    [DoesNotReturn]
    private void ReportErrorAndRethrow(Exception exc, string operation)
    {
        LogErrorRedisOperation(exc, operation, _streamQueueIdName);
        throw new AggregateException($"Error doing {operation} for Redis stream {_streamQueueIdName}", exc);
    }

    [LoggerMessage(
        EventId = (int)ErrorCode.StreamProviderManagerBase,
        Level = LogLevel.Error,
        Message = "Error doing {Operation} for Redis stream {StreamName}"
    )]
    private partial void LogErrorRedisOperation(Exception exception, string operation, string streamName);
}
