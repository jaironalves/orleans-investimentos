namespace Orleans.Investimentos.Silo.Graos.Worker.States
{
    [GenerateSerializer]
    [Alias(nameof(ExecucaoWorkerItemState))]
    public class ExecucaoWorkerItemState
    {
        [Id(0)]
        public DateOnly Data { get; set; }

        [Id(1)]
        public string Valor { get; set; }
    }
}
