using Orleans.Investimentos.Silo.Graos.Base.Investidor.State;
using System.Text.Json.Serialization;

namespace Orleans.Investimentos.Silo.Graos.InvestidorRv.State;

[GenerateSerializer]
[Alias(nameof(InvestidorPosicaoRvState))]
public class InvestidorPosicaoRvState : InvestidorPosicaoState
{

    

    //[Id(0)]
    //public string Ativo { get; set; } = default!;

    //[Id(1)]
    //public decimal Quantidade { get; set; }
}
