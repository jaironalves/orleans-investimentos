namespace Orleans.Investimentos.Silo.Graos.Worker.States
{
    [GenerateSerializer]
    [Alias(nameof(ExecucaoWorkerState))]
    public class ExecucaoWorkerState
    {
        [Id(0)]
        public List<ExecucaoWorkerItemState> Itens { get; set; } = [];
    }
}
