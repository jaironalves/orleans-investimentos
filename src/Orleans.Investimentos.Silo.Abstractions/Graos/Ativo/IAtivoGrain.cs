using Orleans.Investimentos.Silo.Abstractions.Graos.Ativo.Models;

namespace Orleans.Investimentos.Silo.Abstractions.Graos.Ativo;

[Alias("Orleans.Investimentos.Graos.Abstractions.Ativo.IAtivoGrain")]
public interface IAtivoGrain : IGrainWithStringKey
{
    [Alias(nameof(AtualizarPrecoAsync))]
    Task AtualizarPrecoAsync(decimal preco);

    [Alias(nameof(ObterPrecoAsync))]
    Task<decimal> ObterPrecoAsync();

    [Alias(nameof(ObterAsync))]
    Task<AtivoModel> ObterAsync();
}
