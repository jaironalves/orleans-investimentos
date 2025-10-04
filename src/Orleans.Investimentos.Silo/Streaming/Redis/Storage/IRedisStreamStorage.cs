using StackExchange.Redis;

namespace Orleans.Investimentos.Silo.Streaming.Redis.Storage;

public interface IRedisStreamStorage
{
    Task InitAsync();

    Task AddEntriesAsync(IEnumerable<NameValueEntry[]> entries);    

    Task<IEnumerable<StreamEntry>> GetEntriesAsync(RedisValue? position = null, int? count = null);

    Task EntryDeliveredAsync(RedisValue messageId);
}
