using Microsoft.Extensions.Options;
using Orleans.Clustering.Redis;
using Orleans.Investimentos.Silo.Storage;
using Orleans.Persistence;
using StackExchange.Redis;

namespace Orleans.Investimentos.Silo;

public class PostSingleMultiplexerConnection(IServiceProvider serviceProvider) 
    : IPostConfigureOptions<RedisClusteringOptions>,
      IPostConfigureOptions<RedisStorageOptions>
{    
    public void PostConfigure(string? name, RedisClusteringOptions options)
    {        
        options.CreateMultiplexer = (_) =>
        {
            var connectionMultiplexer = serviceProvider.GetRequiredKeyedService<IConnectionMultiplexer>("ClusterConnectionMultiplexer");
            return Task.FromResult(connectionMultiplexer);
        };
    }

    public void PostConfigure(string? name, RedisStorageOptions options)
    {
        options.CreateMultiplexer = (_) =>
        {
            var connectionMultiplexer = serviceProvider.GetRequiredKeyedService<IConnectionMultiplexer>("StorageConnectionMultiplexer");
            return Task.FromResult(connectionMultiplexer);
        };
    }
}
