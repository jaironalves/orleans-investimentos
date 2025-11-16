namespace Orleans.Investimentos.Silo.Graos.Extensions
{
    internal static class GrainExtensions
    {
        public static async Task ProcessInParallelAsync<T>(
            this Grain grain,
            IEnumerable<T> items,
            Func<T, CancellationToken, Task> body,
            int chunkSize = 100,
            Func<Task> onStartAsync = null,
            Func<int, Task> onEndAsync = null,
            ParallelOptions options = null)
        {
            if (items is null)
                return;

            options ??= new ParallelOptions
            {
                MaxDegreeOfParallelism = -1,
                TaskScheduler = TaskScheduler.Current
            };

            int processed = 0;

            if (onStartAsync is not null)
                await onStartAsync();

            try
            {
                await Parallel.ForEachAsync(items, options, async (item, ct) =>
                {                    
                    await body(item, ct);
                    
                    Interlocked.Increment(ref processed);

                    if (processed % chunkSize == 0)
                    {
                        //grain.DelayDeactivation(TimeSpan.FromMinutes(5));
                        await Task.Yield();
                    }
                });
            }
            finally
            {
                if (onEndAsync is not null)
                    await onEndAsync(processed);
            }
        }
    }
}
