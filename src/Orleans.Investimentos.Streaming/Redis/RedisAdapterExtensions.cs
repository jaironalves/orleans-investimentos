using Orleans.Hosting;

namespace Orleans.Investimentos.Streaming.Redis
{
    public static class RedisAdapterExtensions
    {
        public static ISiloBuilder AddRedisStreamsOld(this ISiloBuilder builder, string providerName, string redisConnectionString, int numQueues = 8)
        {
            // Based on MichaelSL's Universley.OrleansContrib.StreamsProvider.Redis
            //https://github.com/MichaelSL/Universley.OrleansContrib.StreamsProvider.Redis/blob/main/example/Server/Program.cs

            // Based on MiloszKrajewski's K4os.Orleans.Streaming.Redis
            //https://github.com/MiloszKrajewski/K4os.Orleans.Streaming.Redis/blob/main/src/K4os.Orleans.Streaming.Redis/Streams/RedisBatchContainer.cs

            builder
                .AddPersistentStreams(providerName, 
                    RedisStreamAdapterFactory.Create,
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
