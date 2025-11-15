using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Options;
using Orleans.Clustering.Redis;
using Orleans.Configuration;
using Orleans.Investimentos.Silo.Serialization;
using Orleans.Investimentos.Silo.Storage;
using Orleans.Investimentos.Silo.Streaming.Redis;
using Orleans.Investimentos.Silo.Streaming.Redis.Hosting;
using StackExchange.Redis;

namespace Orleans.Investimentos.Silo.Extensions;

public static class OrleansExtensions
{

    public static async Task<IConnectionMultiplexer> DefaultCreateMultiplexer(RedisClusteringOptions options)
    {
        return await ConnectionMultiplexer.ConnectAsync(options.ConfigurationOptions);
    }

    public static IHostApplicationBuilder AddOrleans(this IHostApplicationBuilder builder)
    {

                     

        builder.Services
            .AddSingleton<IPostConfigureOptions<RedisClusteringOptions>, PostSingleMultiplexerConnection>();

        var redisConnectionString = builder.Configuration.GetConnectionString("silo-redis");
        var redisOptions = ConfigurationOptions.Parse(redisConnectionString);
        redisOptions.CommandMap = CommandMap.Create(["SUBSCRIBE"], false);
        redisOptions.ClientName = "InvestimentosSilo-Redis";

        //builder.Services.AddSingleton<IConnectionMultiplexer>(connection);
        builder.Services
            .AddSingleton<IConnectionMultiplexer>(sp =>
            {
                
                

                var connection = ConnectionMultiplexer.Connect(redisOptions);
                return connection;
            })
            .AddKeyedSingleton<IConnectionMultiplexer>("ClusterConnectionMultiplexer", (sp, _) =>
            {
                var connectionMultiplexer = sp.GetRequiredService<IConnectionMultiplexer>();
                return new ConnectionMultiplexerWrapper(connectionMultiplexer, 0);
            })
            .AddKeyedSingleton<IConnectionMultiplexer>("StorageConnectionMultiplexer", (sp, _) =>
            {
                var connectionMultiplexer = sp.GetRequiredService<IConnectionMultiplexer>();
                return new ConnectionMultiplexerWrapper(connectionMultiplexer, 1);
            });


        builder
            .UseOrleans(silo =>
            {
                

                var configuration = silo.Configuration;
                var redisConnectionString = configuration.GetConnectionString("silo-redis");

                var porta = 10000;//new Random().Next(10001, 10100);
                //var portaGateway = new Random().Next(20001, 20100);
                var portaGateway = 30000;

                Console.WriteLine($"Starting Orleans Silo on port {porta} with gateway port {portaGateway} and Redis connection string {redisConnectionString}");
                                
                silo
                    .ConfigureEndpoints(porta, portaGateway, listenOnAnyHostAddress: true)
                    //.UseRedisClustering((Action<RedisClusteringOptions>?)null);
                    .UseRedisClustering(opt =>
                    {
                        //opt.CreateRedisKey
                        opt.ConfigurationOptions = redisOptions;
                        //opt.ConfigurationOptions.DefaultDatabase = 0;
                        //opt.CreateMultiplexer = (_) => Task.FromResult<IConnectionMultiplexer>(new ConnectionMultiplexerWrapper(connection, 0));
                    });

                silo.Configure<ClusterOptions>(options =>
                    {
                        options.ClusterId = "investimentosCluster";
                        options.ServiceId = "InvestimentosService";
                    });

                silo
                  //.AddRedisGrainStorage("Investimentos");
                  .AddRedisGrainStorage("Investimentos", opt =>
                  {   
                      opt.ConfigurationOptions = redisOptions;
                      opt.GrainStorageSerializer = new SystemTextJsonStorageSerializer();
                     
                      //   // opt.ConfigurationOptions.DefaultDatabase = 1;
                      //    //opt.CreateMultiplexer = (_) => Task.FromResult<IConnectionMultiplexer>(new ConnectionMultiplexerWrapper(connection, 1));
                  })
                  .AddRedisGrainStorage("PubSubStore", opt =>
                  {
                      opt.ConfigurationOptions = redisOptions;
                  });

                silo
                   .AddRedisStreams("AtivoPrecoStream", opt =>
                   {
                       opt.ConfigurationOptions = redisOptions;
                       opt.MaxStreamLength = 10;
                       opt.TrimTimeMinutes = 2;
                       opt.CreateMultiplexer = (sp, opt) =>
                       {
                           var connectionMultiplexer = sp.GetRequiredService<IConnectionMultiplexer>();
                           return Task.FromResult(connectionMultiplexer);
                       };
                   });


                silo
                     .Services.AddOptions<RedisReminderTableOptions>()
                     .Configure<IServiceProvider>((options, sp) =>
                     {
                         var connectionMultiplexer = sp.GetRequiredService<IConnectionMultiplexer>();
                         options.CreateMultiplexer = (_) => Task.FromResult(connectionMultiplexer);
                         options.ConfigurationOptions = redisOptions;
                     });

                silo.UseRedisReminderService(opt => { });


                //silo
                  //.AddMemoryStreams("AtivoPrecoStream")
                  //.AddMemoryGrainStorage("PubSubStore");

                //silo.AddStreamFilter<AtivoStreamFilter>("AtivoPrecoFilter");

                silo.Configure<GrainCollectionOptions>(options =>
                {
                    options.CollectionAge = TimeSpan.FromMinutes(2);
                });

                silo.UseDashboard(x => x.HostSelf = false);
            });

        return builder;
    }
}
