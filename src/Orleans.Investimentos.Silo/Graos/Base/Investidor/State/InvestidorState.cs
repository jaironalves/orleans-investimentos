using Orleans.Investimentos.Silo.Serialization;
using System.Text.Json.Serialization;

namespace Orleans.Investimentos.Silo.Graos.Base.Investidor.State;

[GenerateSerializer]
[Alias(nameof(InvestidorPosicaoState))]
public class InvestidorPosicaoState
{
    [Id(0)]
    public string Ativo { get; set; } = string.Empty;
    [Id(1)]
    public decimal Quantidade { get; set; }
    [Id(2)]
    public decimal QuantidadeBloqueada { get; set; }
}

//[GenerateSerializer]
//[Alias(nameof(InvestidorState))]
//public class InvestidorState
//{
//    [Id(0)]
//    public string Nome { get; set; } = string.Empty;

//    [Id(1)]
//    //[JsonPropertyName("PosicoesRv")]
//    public Dictionary<string, InvestidorPosicaoState> Posicoes { get; set; } = [];
//}

[GenerateSerializer]
[Alias(nameof(InvestidorState<TInvestidorPosicaoState>) + "<" + nameof(InvestidorPosicaoState) + ">")]
public class InvestidorState<TInvestidorPosicaoState>
    where TInvestidorPosicaoState : InvestidorPosicaoState, new()
{
    [Id(0)]
    public string Nome { get; set; } = string.Empty;

    [Id(1)]
    //[JsonPropertyNamesLegacy("PosicoesRv")]
    public Dictionary<string, TInvestidorPosicaoState> Posicoes { get; set; } = [];

    //[JsonPropertyName("PosicoesRv")]
    ////[JsonIgnore(Condition = JsonIgnoreCondition.)]
    //public Dictionary<string, TInvestidorPosicaoState>? PosicoesRv
    //{     
    //    set => Posicoes = value ?? [];
    //}
}

