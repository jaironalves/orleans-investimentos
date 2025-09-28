using Orleans.Investimentos.Silo.Abstractions.Graos.Ativo.Models;
using Orleans.Streams.Filtering;

namespace Orleans.Investimentos.Silo.Graos.Ativo.Streams;

public class AtivoStreamFilter : IStreamFilter
{
    public bool ShouldDeliver(StreamId streamId, object item, string filterData)
    {
        if (item is AtivoPrecoStreamModel preco)
        {
            var ativoDesejado = filterData;
            return preco.Ativo == ativoDesejado;
        }

        return false;
    }
}
