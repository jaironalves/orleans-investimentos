using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Orleans.Investimentos.Silo.Graos.Base
{
    internal class GrainStringKey : Grain, IGrainWithStringKey
    {
        internal GrainStringKey() { }

        internal GrainStringKey(IGrainContext grainContext) : base(grainContext)
        {

        }

        public async Task ForEachAsync<T>(IEnumerable<T> items,
            Func<T, CancellationToken, Task> itemProcessAsync,
            TimeSpan? tempoParaReciclar = null,
            int? totalParaReciclar = null,
            Func<Task> onStartAsync = null,
            Func<int, Task> onEndAsync = null,
            TimeSpan? tempoIncrementoAtivacaoReciclar = null)
        {
            if (items is null)
                return;

            var cicloInicio = Stopwatch.GetTimestamp();
            long totalTicksCiclo = 0;

            var usarTempo = tempoParaReciclar.HasValue;
            var usarContador = totalParaReciclar.HasValue;
            
            var totalTicksReciclar = tempoParaReciclar?.Ticks ?? 0;
            
            if (onStartAsync is not null)
                await onStartAsync();

            int delayCount = 0;

            int pauseFlag = 0;

            int contador = 0;
            var incrementoAtivacao = tempoIncrementoAtivacaoReciclar ?? TimeSpan.FromMinutes(1);

            var options = new ParallelOptions
            {
                MaxDegreeOfParallelism = -1,
                TaskScheduler = TaskScheduler.Current
            };
            await Parallel.ForEachAsync(items, options, async (item, ct) =>
            {
                await itemProcessAsync(item, ct);

                Interlocked.Increment(ref contador);

                bool precisaReciclar = false;

                if (usarTempo)
                {
                    totalTicksCiclo = Stopwatch.GetTimestamp() - cicloInicio;
                    if (totalTicksCiclo > totalTicksReciclar)
                        precisaReciclar = true;
                }

                if (!precisaReciclar && usarContador)
                {
                    if (contador >= totalParaReciclar.Value)
                        precisaReciclar = true;
                }

                if (precisaReciclar && (Interlocked.CompareExchange(ref pauseFlag, 1, 0) == 0))
                {
                    try
                    {
                        DelayDeactivation(incrementoAtivacao);

                        await Task.Yield();

                        cicloInicio = Stopwatch.GetTimestamp();
                        Interlocked.Exchange(ref contador, 0);

                        Interlocked.Increment(ref delayCount);
                    }
                    finally
                    {
                        Interlocked.Exchange(ref pauseFlag, 0);
                    }
                }
            });

            if (onEndAsync is not null)
                await onEndAsync(delayCount);

        }
    }
}
