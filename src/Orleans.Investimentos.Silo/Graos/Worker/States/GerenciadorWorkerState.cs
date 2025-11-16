namespace Orleans.Investimentos.Silo.Graos.Worker.States
{
    [GenerateSerializer]
    [Alias(nameof(GerenciadorWorkerState))]
    public class GerenciadorWorkerState
    {
        [Id(0)]
        public DateOnly Data { get; set; }
    }
}
