
using Orleans.Investimentos.Client.Observers;
using Orleans.Investimentos.Silo.Abstractions.Graos.Ativo.Models;
using Orleans.Streams;

namespace Orleans.Investimentos.Client.Hosted
{
    public class AtivosPrecosBackgroundService(IClusterClient clusterClient, ILoggerFactory loggerFactory) : BackgroundService
    {
        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            var streamProvider = clusterClient.GetStreamProvider("AtivoPrecoStream");
            var stream = streamProvider.GetStream<AtivoPrecoStreamModel>("AtivoPreco", "PETR4");

            var logger = loggerFactory.CreateLogger<AtivoObserver>();
            await stream.SubscribeAsync(new AtivoObserver(logger));
        }
    }
}
