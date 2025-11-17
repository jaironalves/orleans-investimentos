using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Configuration;
using Orleans.Hosting;
using Orleans.Streams;

namespace Orleans.Investimentos.Streaming.Redis.Hosting.Configurator;

public class ClusterClientRedisStreamConfigurator : ClusterClientPersistentStreamConfigurator
{
    public ClusterClientRedisStreamConfigurator(string name, IClientBuilder clientBuilder)
        : base(name, clientBuilder, RedisStreamAdapterFactory.Create)
    {
        ConfigureDelegate(services =>
        {
            services
                .ConfigureNamedOptionForLogging<RedisStreamOptions>(name)
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

    public ClusterClientRedisStreamConfigurator ConfigureQueueDataAdapter(Func<IServiceProvider, string, IQueueDataAdapter<string, IBatchContainer>> factory)
    {
        this.ConfigureComponent(factory);
        return this;
    }

    public ClusterClientRedisStreamConfigurator ConfigureQueueDataAdapter<TQueueDataAdapter>()
        where TQueueDataAdapter : class, IQueueDataAdapter<string, IBatchContainer>
    {
        this.ConfigureComponent<IQueueDataAdapter<string, IBatchContainer>>((sp, n) => ActivatorUtilities.CreateInstance<TQueueDataAdapter>(sp));
        return this;
    }

    internal void PostConfigureComponents()
    {
        ConfigureDelegate(services => RedisStreamAdapterFactory.PostConfigureDefaults(services, Name));
    }
}
