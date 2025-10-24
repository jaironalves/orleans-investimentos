using Orleans.Streams;
using StackExchange.Redis;
using System.Threading;

namespace Orleans.Investimentos.Silo.Streaming.Redis.Storage;

internal class RedisStorage(IConnectionMultiplexer connectionMultiplexer,
    RedisKey streamKey, string streamName, ILoggerFactory loggerFactory) : IRedisStorage
{
    private const string GROUP_NAME = "consumer";

    private readonly ILogger<RedisStorage> logger = loggerFactory.CreateLogger<RedisStorage>();

    private readonly IDatabase database = connectionMultiplexer.GetDatabase();

    public async Task InitAsync()
    {
        try
        {
            await database
                .StreamCreateConsumerGroupAsync(streamKey, GROUP_NAME, "$", true);
        }
        catch (Exception ex) when (ex.Message.Contains("name already exists")) 
        { 
            logger.LogInformation("Consumer group 'consumer' already exists for stream {StreamName}", streamName);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Error initializing stream {StreamName}", streamKey);
        }
    }

    public async Task AddEntriesAsync(IEnumerable<NameValueEntry[]> entries)
    {
        foreach (var entryValues in entries)
        {
            await database.StreamAddAsync(streamKey, entryValues);
        }
    }

    public async Task<IEnumerable<StreamEntry>> GetEntriesAsync(RedisValue? position = null, int? count = null)
    {
        var entries = await database.StreamReadGroupAsync(streamKey, GROUP_NAME, streamName, position ?? ">", count);
        return entries;
    }

    public async Task EntryDeliveredAsync(RedisValue messageId)
    {
        await database.StreamAcknowledgeAsync(streamKey, GROUP_NAME, messageId);
    }    
}
