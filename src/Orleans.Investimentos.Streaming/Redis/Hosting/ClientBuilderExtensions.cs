using Microsoft.Extensions.Options;
using Orleans.Hosting;

namespace Orleans.Investimentos.Streaming.Redis.Hosting;

public static class ClientBuilderExtensions
{   
    /// <summary>
    /// Configure cluster client to use Redis persistent streams with default settings
    /// </summary>
    public static IClientBuilder AddRedisStreams(this IClientBuilder builder, string name, Action<RedisStreamOptions> configureOptions)
    {
        builder.AddRedisStreams(name, ob => ob.Configure(configureOptions));
        return builder;
    }

    /// <summary>
    /// Configure cluster client to use Redis persistent streams with default settings
    /// </summary>
    public static IClientBuilder AddRedisStreams(this IClientBuilder builder, string name, Action<OptionsBuilder<RedisStreamOptions>> configureOptionsBuilder)
    {
        builder.AddRedisStreams(name, cb =>
            cb.ConfigureRedis(configureOptionsBuilder));
        return builder;
    }

    /// <summary>
    /// Configure cluster client to use Redis persistent streams.
    /// </summary>
    public static IClientBuilder AddRedisStreams(this IClientBuilder builder, string name, Action<ClusterClientRedisStreamConfigurator> configure)
    {
        var configurator = new ClusterClientRedisStreamConfigurator(name, builder);
        configure?.Invoke(configurator);
        configurator.PostConfigureComponents();
        return builder;
    }
}

