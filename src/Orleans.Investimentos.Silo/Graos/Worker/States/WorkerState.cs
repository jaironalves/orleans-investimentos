namespace Orleans.Investimentos.Silo.Graos.Worker.States
{
    [GenerateSerializer]
    [Alias(nameof(WorkerState))]
    public class WorkerState
    {
        [Id(0)]
        public Dictionary<DateOnly, List<string>> Registros { get; set; } = [];
    }
}
