using Microsoft.Extensions.Options;
using Orleans.Configuration;
using Orleans.Streams;

namespace Orleans.Investimentos.Silo.Streaming.Redis.Hosting.Configurator;

public class SiloRedisStreamConfigurator : SiloPersistentStreamConfigurator
{
    public SiloRedisStreamConfigurator(string name, Action<Action<IServiceCollection>> configureDelegate) : 
        base(name, configureDelegate, RedisStreamAdapterFactory.Create)
    {
        ConfigureDelegate(services =>
        {
            RedisStreamAdapterFactory
                .AddKeyedServices(services, name)
                .ConfigureNamedOptionForLogging<RedisStreamOptions>(name)
                .ConfigureNamedOptionForLogging<SimpleQueueCacheOptions>(name)
                .ConfigureNamedOptionForLogging<HashRingStreamQueueMapperOptions>(name);
        });
    }

    public SiloRedisStreamConfigurator ConfigureRedis(Action<OptionsBuilder<RedisStreamOptions>> configureOptions)
    {
        this.Configure(configureOptions);
        return this;
    }

    public SiloRedisStreamConfigurator ConfigureCache(int cacheSize = SimpleQueueCacheOptions.DEFAULT_CACHE_SIZE)
    {
        this.Configure<SimpleQueueCacheOptions>(ob => ob.Configure(options => options.CacheSize = cacheSize));
        return this;
    }

    public SiloRedisStreamConfigurator ConfigurePartitioning(int numOfparitions = HashRingStreamQueueMapperOptions.DEFAULT_NUM_QUEUES)
    {
        this.Configure<HashRingStreamQueueMapperOptions>(ob => ob.Configure(options => options.TotalQueueCount = numOfparitions));
        return this;
    }
}
