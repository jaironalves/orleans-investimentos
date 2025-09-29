
using Microsoft.Extensions.Options;

namespace Orleans.Investimentos.Silo.Streaming.Redis
{
    public interface IRedisServiceProvider : IServiceProvider
    {
        string Name { get; }

        TOption GetOptions<TOption>() where TOption : class, new();

        T GetNamedService<T>() where T : notnull;
    }

    public class RedisServiceProvider(IServiceProvider serviceProvider, string name) : IRedisServiceProvider
    {
        public string Name => name;

        public object? GetService(Type serviceType)
        {
            serviceProvider.GetService<IRedisProviderName>()?.Name.Equals(Name, StringComparison.OrdinalIgnoreCase);

            return serviceProvider.GetService(serviceType);
        }

        public T GetNamedService<T>() where T : notnull
        {
            return serviceProvider
                .GetRequiredKeyedService<T>(Name);            
        }

        public TOption GetOptions<TOption>()
            where TOption : class, new()
        {
            return serviceProvider
                .GetRequiredService<IOptionsMonitor<TOption>>()
                .Get(Name);
        }
    }
}
