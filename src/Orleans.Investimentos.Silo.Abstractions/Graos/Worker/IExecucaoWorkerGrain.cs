using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Orleans.Investimentos.Silo.Abstractions.Graos.Worker
{
    [Alias(nameof(IExecucaoWorkerGrain))]
    public interface IExecucaoWorkerGrain : IGrainWithStringKey
    {
        Task AgendarAsync(Dictionary<DateOnly, List<string>> registros);
    }
}
