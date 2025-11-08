using Microsoft.Extensions.Options;

namespace Orleans.Investimentos.Silo.Streaming.Redis;

internal class RedisStreamServiceProvider(IServiceProvider serviceProvider, string name) : IServiceProvider
{
    public string Name => name;

    public object? GetService(Type serviceType)
    {
        return serviceProvider.GetService(serviceType);
    }        

    public TOption GetOptions<TOption>()
        where TOption : class, new()
    {
        return serviceProvider
            .GetRequiredService<IOptionsMonitor<TOption>>()
            .Get(Name);
    }
}
