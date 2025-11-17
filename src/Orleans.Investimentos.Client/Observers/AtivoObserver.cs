using Orleans.Investimentos.Silo.Abstractions.Graos.Ativo.Models;
using Orleans.Streams;

namespace Orleans.Investimentos.Client.Observers
{
    public class AtivoObserver(ILogger<AtivoObserver> logger) : IAsyncObserver<AtivoPrecoStreamModel>
    {
        public Task OnErrorAsync(Exception ex)
        {
            logger.LogError(ex, "Erro no stream de preços dos ativos");
            return Task.CompletedTask;
        }

        public Task OnNextAsync(AtivoPrecoStreamModel item, StreamSequenceToken token = null)
        {
            logger.LogInformation("Ativo {Ativo} atualizado com preço {Preco}", item.Ativo, item.Preco);
            return Task.CompletedTask;
        }
    }
}
