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
            //await ExecutarAsync(correlationId);
            await ExecutarAsyncV2(correlationId);
            logger.LogInformation("ReceiveReminder {Key} finalizado: {reminderName}", this.GetPrimaryKeyString(), reminderName);
            //return Task.CompletedTask;
        }

        public override Task OnDeactivateAsync(DeactivationReason reason, CancellationToken cancellationToken)
        {
            logger.LogWarning("ExecucaoWorkerGrain.OnDeactivateAsync {Key} - Reason: {Reason}", this.GetPrimaryKeyString(), reason);
            return base.OnDeactivateAsync(reason, cancellationToken);
        }

        private async Task ExecutarAsyncV2(string correlationId)
        {
            var itens = persistentState.State.Itens ?? new List<ExecucaoWorkerItemState>();
            await ForEachAsync(itens, async (item, ct) =>
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
            },
            onEndAsync: delay =>
            {
                logger.LogInformation("ExecucaoWorkerGrain.ExecutarAsync {Key} loop {Count}", this.GetPrimaryKeyString(), delay);
                return Task.CompletedTask;
            },
            tempoParaReciclar: TimeSpan.FromMilliseconds(100));
        }

        private async Task ExecutarAsync(string correlationId)
        {
            logger.LogInformation("ExecucaoWorkerGrain.ExecutarAsync {Key} Total {Total}: {correlationId}", this.GetPrimaryKeyString(), persistentState.State.Itens.Count, correlationId);

            int processed = 0;

            // Ajuste o grau de paralelismo conforme sua capacidade do silo
            //var parallelOptions = new ParallelOptions
            //{
            //    MaxDegreeOfParallelism = Math.Max(1, Environment.ProcessorCount * 2)
            //};
            var itens = persistentState.State.Itens ?? new List<ExecucaoWorkerItemState>();

            DelayDeactivation(TimeSpan.FromMinutes(5));
            
            
            var paprallelOptions = new ParallelOptions
            {
                MaxDegreeOfParallelism = -1,
                TaskScheduler = TaskScheduler.Current
            };

            await Parallel.ForEachAsync(itens, paprallelOptions, async (item, ct) =>
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
                // Incrementa o contador de processados de forma thread-safe
                Interlocked.Increment(ref processed);
                // A cada 100 itens processados, renova a ativação e cede ao scheduler
                if (processed % 100 == 0)
                {
                    this.DelayDeactivation(TimeSpan.FromMinutes(5));
                    await Task.Yield();
                }
            });

            //const int batchSize = 50;
            //for (int offset = 0; offset < itens.Count; offset += batchSize)
            //{
            //    var batch = itens.Skip(offset).Take(batchSize).ToList();

            //    // Cria tarefas para o batch e executa em paralelo com Task.WhenAll
            //    var tasks = batch.Select(async item =>
            //    {
            //        try
            //        {
            //            var gerenciadorGrain = GrainFactory.GetGrain<IGerenciadorWorkerGrain>(item.Valor);
            //            await gerenciadorGrain.ExecutarTarefaAsync(correlationId, item.Data);
            //        }
            //        catch (Exception ex)
            //        {
            //            logger.LogError(ex, "Erro {Key} - {Correlation}", this.GetPrimaryKeyString(), correlationId);
            //        }
            //    }).ToArray();

            //    await Task.WhenAll(tasks);

            //    // Após cada batch: renova a ativação e cede ao scheduler
            //    this.DelayDeactivation(TimeSpan.FromMinutes(5));
            //    await Task.Yield();
            //}

            //logger.LogInformation("ExecucaoWorkerGrain.ExecutarAsync {Key} Limpeza: {correlationId}", this.GetPrimaryKeyString(), correlationId);

            logger.LogInformation("ExecucaoWorkerGrain.ExecutarAsync {Key} Limpeza: {correlationId}", this.GetPrimaryKeyString(), correlationId);

            // Limpar itens após a execução
            persistentState.State.Itens.Clear();
            await persistentState.WriteStateAsync();

            logger.LogInformation("ExecucaoWorkerGrain.ExecutarAsync {Key} finalizado para CorrelationId: {correlationId}", this.GetPrimaryKeyString(), correlationId);
        }
    }
}
