using Orleans.Investimentos.Silo.Abstractions.Graos.Posicao.Models;

namespace Orleans.Investimentos.Silo.Abstractions.Graos.Posicao;

[Alias("Orleans.Investimentos.Graos.Abstractions.Posicao.IPosicaoGrain")]
public interface IPosicaoGrain : IGrainWithStringKey
{
    [Alias(nameof(AtualizarQuantidadeAsync))]
    Task AtualizarQuantidadeAsync(int quantidade);

    [Alias(nameof(ObterAsync))]
    Task<PosicaoModel> ObterAsync();
}
