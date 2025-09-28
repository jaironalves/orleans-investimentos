var builder = DistributedApplication.CreateBuilder(args);

var siloRedis = builder
    .AddRedis("silo-redis", port: 6380)
    .WithDataVolume("orleans-investimentos-redis-data")
    .WithPersistence(TimeSpan.FromSeconds(10), 5);

var silo = builder
        .AddProject<Projects.Orleans_Investimentos_Silo>("orleans-investimentos-silo")
        .WithReference(siloRedis)
        .WithReplicas(1)
        .WithExternalHttpEndpoints()
        .WithHttpHealthCheck("/health");

builder.Build().Run();
