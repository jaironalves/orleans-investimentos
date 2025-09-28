using Orleans.Serialization;
using StackExchange.Redis;

namespace Orleans.Investimentos.Silo.Streaming.Redis
{
    public static class RedisAdapterExtensions
    {
        public static ISiloBuilder AddRedisStreams(this ISiloBuilder builder, string providerName, string redisConnectionString, int numQueues = 8)
        {
            builder
                .AddPersistentStreams(providerName, 
                    RedisAdapterFactory.Create,
                    null);

            //builder.AddPersistentStreams(providerName, (sp, name) =>
            //{
            //    var serializer = sp.GetRequiredService<Serializer>();
            //    var redis = ConnectionMultiplexer.Connect(redisConnectionString);
            //    return new RedisAdapterFactory(name, redis, serializer, numQueues);
            //});

            return builder;
        }

    }
}
