using Microsoft.Extensions.Options;
using Orleans.Configuration;

namespace Orleans.Investimentos.Silo.Streaming.Redis.Hosting.Configurator;

public class ClusterClientRedisStreamConfigurator : ClusterClientPersistentStreamConfigurator
{
    public ClusterClientRedisStreamConfigurator(string name, IClientBuilder clientBuilder) 
        : base(name, clientBuilder, RedisAdapterFactory.Create)
    {
        clientBuilder
            .ConfigureServices(services =>
            {
                services.ConfigureNamedOptionForLogging<RedisStreamOptions>(name)
                .ConfigureNamedOptionForLogging<HashRingStreamQueueMapperOptions>(name);
            });
    }

    public ClusterClientRedisStreamConfigurator ConfigureRedis(Action<OptionsBuilder<RedisStreamOptions>> configureOptions)
    {
        this.Configure(configureOptions);
        return this;

    }

    public ClusterClientRedisStreamConfigurator ConfigurePartitioning(int numOfparitions = HashRingStreamQueueMapperOptions.DEFAULT_NUM_QUEUES)
    {
        this.Configure<HashRingStreamQueueMapperOptions>(ob => ob.Configure(options => options.TotalQueueCount = numOfparitions));
        return this;
    }
}
