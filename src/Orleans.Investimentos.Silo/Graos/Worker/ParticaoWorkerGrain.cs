using Orleans.Investimentos.Silo.Abstractions.Graos.Ativo.Models;
using Orleans.Investimentos.Silo.Abstractions.Graos.Worker;
using Orleans.Investimentos.Silo.Graos.Base;
using Orleans.Investimentos.Silo.Graos.Worker.States;
using Orleans.Streams;

namespace Orleans.Investimentos.Silo.Graos.Worker
{
    internal class ParticaoWorkerGrain(
       [PersistentState("particaoWorkerState", "Investimentos")]
       IPersistentState<WorkerState> persistentState) : GrainStringKey, IParticaoWorkerGrain
    {
        private IAsyncStream<bool> workerStream;

        public override Task OnActivateAsync(CancellationToken cancellationToken)
        {
            workerStream = this
                            .GetStreamProvider("WorkerStream")
                            .GetStream<bool>("Worker", this.GetPrimaryKeyString());

            return Task.CompletedTask;
        }

        public async Task AdicionarAsync(string valor, DateOnly data)
        {
            persistentState.State.Registros.TryGetValue(data, out var listaValores);
            if (listaValores == null)
            {
                listaValores = [];
                persistentState.State.Registros[data] = listaValores;
            }

            if (!listaValores.Contains(valor))
            {
                listaValores.Add(valor);
            }

            await persistentState.WriteStateAsync();
        }

        public async Task AgendarAsync()
        {
            var particao = this.GetPrimaryKeyString();
            var workerGrain = GrainFactory.GetGrain<IExecucaoWorkerGrain>(particao);
            await workerGrain.AgendarAsync(persistentState.State.Registros);            
        }
    }
}
