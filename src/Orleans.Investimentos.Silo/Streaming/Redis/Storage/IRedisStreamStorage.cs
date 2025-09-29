using StackExchange.Redis;

namespace Orleans.Investimentos.Silo.Streaming.Redis.Storage;

public interface IRedisStreamStorage
{
    Task AddEntriesAsync(RedisKey key, IEnumerable<NameValueEntry[]> entries);    

    Task<IEnumerable<StreamEntry>> GetEntriesAsync(RedisKey key, RedisValue groupName, RedisValue consumerName,
        RedisValue? position = null, int? count = null);

    Task EntryDeliveredAsync(RedisKey key, RedisValue groupName, RedisValue messageId);
}
