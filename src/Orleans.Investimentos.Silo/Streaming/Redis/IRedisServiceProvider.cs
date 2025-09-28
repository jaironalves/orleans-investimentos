
namespace Orleans.Investimentos.Silo.Streaming.Redis
{
    public interface IRedisServiceProvider : IServiceProvider
    {
        string Name { get; }
    }

    public class RedisServiceProvider(IServiceProvider serviceProvider, string name) : IServiceProvider
    {
        public string Name => name;

        public object? GetService(Type serviceType)
        {
            return serviceProvider.GetService(serviceType);
        }
    }
}
