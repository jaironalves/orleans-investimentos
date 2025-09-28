using Microsoft.Extensions.Options;
using Orleans.Configuration;
using Orleans.Streams;

namespace Orleans.Investimentos.Silo.Streaming.Redis.Hosting.Configurator;

public class SiloRedisStreamConfigurator : SiloPersistentStreamConfigurator
{
    public SiloRedisStreamConfigurator(string name, Action<Action<IServiceCollection>> configureDelegate) : 
        base(name, configureDelegate, RedisAdapterFactoryV2.Create)
    {
        ConfigureDelegate(services =>
        {
            services.AddKeyedSingleton<IRedisAdapterFactory>(name, (sp, serviceKey) =>
            {
                var providerName = $"{serviceKey}";
                return new RedisAdapterFactoryV2(sp, new RedisProviderName(providerName));
            });

            services
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
