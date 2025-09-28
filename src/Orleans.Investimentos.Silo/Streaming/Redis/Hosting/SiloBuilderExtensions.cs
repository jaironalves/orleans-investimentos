using Microsoft.Extensions.Options;
using Orleans.Investimentos.Silo.Streaming.Redis.Hosting.Configurator;

namespace Orleans.Investimentos.Silo.Streaming.Redis.Hosting;

public static class SiloBuilderExtensions
{
    /// <summary>
    /// Configure silo to use Redis persistent streams.
    /// </summary>
    public static ISiloBuilder AddRedisStreams(this ISiloBuilder builder, string name, Action<RedisStreamOptions> configureOptions)
    {
        builder.AddRedisStreams(name, cb =>
            cb.ConfigureRedis(ob => ob.Configure(configureOptions)));
        return builder;
    }

    /// <summary>
    /// Configure silo to use Redis persistent streams.
    /// </summary>
    public static ISiloBuilder AddRedisStreams(this ISiloBuilder builder, string name, Action<OptionsBuilder<RedisStreamOptions>> configureOptionsBuilder)
    {   
        builder.AddRedisStreams(name, cb =>
            cb.ConfigureRedis(configureOptionsBuilder));
        return builder;
    }

    /// <summary>
    /// Configure silo to use Redis persistent streams.
    /// </summary>
    public static ISiloBuilder AddRedisStreams(this ISiloBuilder builder, string name, Action<SiloRedisStreamConfigurator> configure)
    {
        var configurator = new SiloRedisStreamConfigurator(name,
            configureServicesDelegate => builder.ConfigureServices(configureServicesDelegate));
        configure?.Invoke(configurator);
        return builder;
    }    
}
