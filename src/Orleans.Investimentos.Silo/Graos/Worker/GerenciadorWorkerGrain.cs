using Orleans.Investimentos.Silo.Abstractions.Graos.Worker;
using Orleans.Investimentos.Silo.Graos.Base;
using Orleans.Investimentos.Silo.Graos.Worker.States;
using System.Security.Cryptography;
using System.Text;

namespace Orleans.Investimentos.Silo.Graos.Worker
{
    internal class GerenciadorWorkerGrain([PersistentState("gerenciadorWorkerState", "Investimentos")]
       IPersistentState<GerenciadorWorkerState> persistentState,
        ILogger<GerenciadorWorkerGrain> logger) : GrainStringKey, IGerenciadorWorkerGrain
    {
        public async Task NotificarAsync(DateOnly data)
        {
            var particao = RetornarParticao();
            var particaoGrain = GrainFactory.GetGrain<IParticaoWorkerGrain>(particao);
            await particaoGrain.AdicionarAsync(this.GetPrimaryKeyString(), data);

            persistentState.State.Data = data;
            await persistentState.WriteStateAsync();
        }

        public async Task AgendarAsync()
        {
            var particao = RetornarParticao();
            var particaoGrain = GrainFactory.GetGrain<IParticaoWorkerGrain>(particao);
            await particaoGrain.AgendarAsync();
        }

        private string RetornarParticao()
        {
            // Obtém a chave primária (pode ser null em cenários inesperados)
            var key = this.GetPrimaryKeyString() ?? string.Empty;

            var valor = key.Length <= 3 ? key : key[^3..];
            var valorPadded = valor.PadLeft(3, '0');

            var bytes = Encoding.UTF8.GetBytes(valorPadded);
            var hash = SHA256.HashData(bytes);

            uint value = BitConverter.ToUInt32(hash, 0);
            int partition = (int)(value % 30u) + 1;
            return partition.ToString();
        }

        public Task ExecutarTarefaAsync(string correlationId, DateOnly data)
        {
            //logger.LogInformation("Iniciando {Grain} tarefa para CorrelationId: {correlationId} na data: {data}", this.GetPrimaryKeyString(), correlationId, data);
            var random = new Random();
            var delay = random.Next(1000, 3000);
            //Thread.Sleep(delay);
            return Task.Delay(delay);
            //return Task.CompletedTask;
            //logger.LogInformation("Tarefa {Grain} concluída para CorrelationId: {correlationId} na data: {data}", this.GetPrimaryKeyString(), correlationId, data);
        }
    }
}
