using Orleans.Investimentos.Silo.Abstractions.Graos.Worker;
using Orleans.Investimentos.Silo.Graos.Base;
using Orleans.Investimentos.Silo.Graos.Worker.States;

namespace Orleans.Investimentos.Silo.Graos.Worker
{
    internal class ExecucaoWorkerGrain(
       [PersistentState("execucaoWorkerState", "Investimentos")]
       IPersistentState<ExecucaoWorkerState> persistentState, ILogger<ExecucaoWorkerGrain> logger) : GrainStringKey, IExecucaoWorkerGrain, IRemindable
    {
        public async Task AgendarAsync(Dictionary<DateOnly, List<string>> registros)
        {
            persistentState.State.Itens = [.. registros
                .SelectMany(kvp => kvp.Value.Select(valor => new ExecucaoWorkerItemState()
                {
                    Data = kvp.Key,
                    Valor = valor
                }))];

            await this.RegisterOrUpdateReminder(
                reminderName: "ExecucaoWorkerReminder",
                dueTime: TimeSpan.FromMinutes(1),
                period: TimeSpan.FromDays(1)
                );

            await persistentState.WriteStateAsync();
        }

        public async Task ReceiveReminder(string reminderName, TickStatus status)
        {
            var correlationId = Guid.NewGuid().ToString();
            logger.LogInformation("ReceiveReminder {Key} chamado: {reminderName}", this.GetPrimaryKeyString(),  reminderName);
            //_ = ExecutarAsync(correlationId);
            await ExecutarAsync(correlationId);
            logger.LogInformation("ReceiveReminder {Key} finalizado: {reminderName}", this.GetPrimaryKeyString(), reminderName);
            //return Task.CompletedTask;
        }

        public override Task OnDeactivateAsync(DeactivationReason reason, CancellationToken cancellationToken)
        {
            logger.LogWarning("ExecucaoWorkerGrain.OnDeactivateAsync {Key} - Reason: {Reason}", this.GetPrimaryKeyString(), reason);
            return base.OnDeactivateAsync(reason, cancellationToken);
        }

        private async Task ExecutarAsync(string correlationId)
        {
            logger.LogInformation("ExecucaoWorkerGrain.ExecutarAsync {Key} Total {Total}: {correlationId}", this.GetPrimaryKeyString(), persistentState.State.Itens.Count, correlationId);

            foreach (var item in persistentState.State.Itens)
            {
                try
                {
                    var gerenciadorGrain = GrainFactory.GetGrain<IGerenciadorWorkerGrain>(item.Valor);
                    await gerenciadorGrain.ExecutarTarefaAsync(correlationId, item.Data);
                }
                catch (Exception ex)
                {
                    logger.LogError(ex, "Erro {Key} - {Correlation}", this.GetPrimaryKeyString(), correlationId);                    
                }
                this.DelayDeactivation(TimeSpan.FromMinutes(5));
                await Task.Yield();
            }

            logger.LogInformation("ExecucaoWorkerGrain.ExecutarAsync {Key} Limpeza: {correlationId}", this.GetPrimaryKeyString(), correlationId);

            // Limpar itens após a execução
            persistentState.State.Itens.Clear();
            await persistentState.WriteStateAsync();

            logger.LogInformation("ExecucaoWorkerGrain.ExecutarAsync {Key} finalizado para CorrelationId: {correlationId}", this.GetPrimaryKeyString(), correlationId);
        }
    }
}
