using Orleans.Clustering.Redis;
using StackExchange.Redis;

namespace Orleans.Investimentos.Silo.Streaming.Redis;

/// <summary>
/// Options for Redis streaming.
/// </summary>
public class RedisStreamOptions
{
    /// <summary>
    /// Gets or sets the Redis client configuration.
    /// </summary>
    [RedactRedisConfigurationOptions]
    public ConfigurationOptions ConfigurationOptions { get; set; } = default!;

    /// <summary>
    /// The delegate used to create a Redis configuration options.
    /// </summary>
    public Func<IServiceProvider, Task<ConfigurationOptions>> CreateConfigurationOptions { get; set; } = DefaultCreateConfigurationOptions;
    
    /// <summary>
    /// The delegate used to create a Redis connection multiplexer.
    /// </summary>
    public Func<RedisStreamOptions, Task<IConnectionMultiplexer>> CreateMultiplexer { get; set; } = DefaultCreateMultiplexer;


    /// <summary>
    /// The default multiplexer creation delegate.
    /// </summary>
    public static async Task<IConnectionMultiplexer> DefaultCreateMultiplexer(RedisStreamOptions options)
    {
        return await ConnectionMultiplexer.ConnectAsync(options.ConfigurationOptions);
    }

    /// <summary>
    /// The default configuration options creation delegate.
    /// </summary>
    private static Task<ConfigurationOptions> DefaultCreateConfigurationOptions(IServiceProvider provider)
    {
        return Task.FromResult(new ConfigurationOptions());
    }
}

internal class RedactRedisConfigurationOptions : RedactAttribute
{
    public override string Redact(object value) => value is ConfigurationOptions cfg ? cfg.ToString(includePassword: false) : base.Redact(value);
}
