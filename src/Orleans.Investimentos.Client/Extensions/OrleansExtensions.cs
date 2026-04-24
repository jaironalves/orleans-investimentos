using Orleans.Configuration;
using StackExchange.Redis;
using Orleans.Investimentos.Streaming.Redis.Hosting;
using Orleans.Clustering.Redis;
using Microsoft.Extensions.Options;
using Orleans.Investimentos.Streaming.Redis;

namespace Orleans.Investimentos.Client.Extensions;

public static class OrleansExtensions
{
    public static IHostApplicationBuilder AddOrleans(this IHostApplicationBuilder builder)
    {

        var redisConnectionString = builder.Configuration.GetConnectionString("silo-redis");
        var redisOptions = ConfigurationOptions.Parse(redisConnectionString);        
        redisOptions.ClientName = "InvestimentosClient-Redis";

        builder
            .Services
            .AddSingleton<IConnectionMultiplexer>(sp =>
            {
                var connection = ConnectionMultiplexer.Connect(redisOptions);
                return connection;
            });

        builder
            .UseOrleansClient(client =>
            {
                client.Configure<ClusterOptions>(options =>
                {
                    options.ClusterId = "investimentosCluster";
                    options.ServiceId = "InvestimentosService";
                });

                client
                    .Services
                    .AddOptions<RedisClusteringOptions>()
                    .Configure<IServiceProvider>((opt, sp) =>
                    {
                        opt.ConfigurationOptions = redisOptions;
                        opt.CreateMultiplexer = (opt) =>
                        {
                            var connectionMultiplexer = sp.GetRequiredService<IConnectionMultiplexer>();
                            return Task.FromResult(connectionMultiplexer);
                        };
                    });

                client
                    .UseRedisClustering(opt => { });

                client
                    .AddRedisStreams("AtivoPrecoStream", (OptionsBuilder<RedisStreamOptions> opt) =>
                    {
                        opt.Configure<IServiceProvider>((opt, sp) =>
                        {
                            opt.ConfigurationOptions = redisOptions;                            
                            opt.CreateMultiplexer = (_) =>
                            {
                                var connectionMultiplexer = sp.GetRequiredService<IConnectionMultiplexer>();
                                return Task.FromResult(connectionMultiplexer);
                            };
                        });                    
                    });
            });        


        return builder;
    }
}
