using StackExchange.Redis;

namespace Orleans.Investimentos.Silo.Streaming.Redis.Storage;

internal class RedisStreamStorage(IRedisServiceProvider provider) : IRedisStreamStorage
{
    private readonly IDatabase database = provider
        .GetRequiredService<IConnectionMultiplexer>()
        .GetDatabase();

    public async Task AddEntriesAsync(RedisKey key, IEnumerable<NameValueEntry[]> entries)
    {
        foreach (var entryValues in entries)
        {
            await database.StreamAddAsync(key, entryValues);
        }
    }

    public async Task<IEnumerable<StreamEntry>> GetEntriesAsync(RedisKey key, RedisValue groupName, RedisValue consumerName, RedisValue? position = null, int? count = null)
    {
        var entries = await database.StreamReadGroupAsync(key, groupName, consumerName, position ?? ">", count);
        return entries;
    }

    public async Task EntryDeliveredAsync(RedisKey key, RedisValue groupName, RedisValue messageId)
    {
        await database.StreamAcknowledgeAsync(key, groupName, messageId);
    }
}
