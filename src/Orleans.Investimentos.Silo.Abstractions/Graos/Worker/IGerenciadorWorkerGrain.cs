using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Orleans.Investimentos.Silo.Abstractions.Graos.Worker
{
    [Alias(nameof(IGerenciadorWorkerGrain))]
    public interface IGerenciadorWorkerGrain : IGrainWithStringKey
    {
        [Alias("AgendarAsync")]
        Task AgendarAsync();

        [Alias("ExecutarTarefaAsync")]
        Task ExecutarTarefaAsync(string correlationId, DateOnly data);

        [Alias("NotificarAsync")]
        Task NotificarAsync(DateOnly data);
    }
}
