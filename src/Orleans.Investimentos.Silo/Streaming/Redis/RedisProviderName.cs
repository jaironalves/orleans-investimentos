namespace Orleans.Investimentos.Silo.Streaming.Redis
{
    public interface IRedisProviderName
    {
        string Name { get; }
    }

    public class RedisProviderName(string name) : IRedisProviderName
    {
        private readonly string name = name;
        public string Name => name;
    }
}
