
namespace Orleans.Investimentos.Silo.Abstractions.Graos.Worker
{
    [Alias(nameof(IParticaoWorkerGrain))]
    public interface IParticaoWorkerGrain : IGrainWithStringKey
    {
        [Alias("AdicionarAsync")]
        Task AdicionarAsync(string valor, DateOnly data);

        [Alias("AgendarAsync")]
        Task AgendarAsync();
    }
}
