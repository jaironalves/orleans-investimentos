using Orleans.Investimentos.Silo.Graos.Base.Investidor.State;
using Orleans.Investimentos.Silo.Serialization;
using System.Text.Json.Serialization;

namespace Orleans.Investimentos.Silo.Graos.InvestidorRv.State;

[GenerateSerializer]
[Alias(nameof(InvestidorRvState))]
[JsonNewtonsoftLegacy]
public class InvestidorRvState : InvestidorState<InvestidorPosicaoRvState>
{
    //[Id(0)]
    //public string Nome { get; set; } = default!;
    //[Id(1)]
    //public Dictionary<string, InvestidorPosicaoRvState> PosicoesRv { get; set; } = [];
    //[Id(0)]
    //[JsonPropertyNamesLegacy("PosicoesRv")]
    //public new Dictionary<string, InvestidorPosicaoRvState> Posicoes { get; set; } = [];


    [JsonInclude]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingDefault)]    
    protected Dictionary<string, InvestidorPosicaoRvState>? PosicoesRv
    {   
        set 
        { 
            if (value != null) 
                Posicoes = value; 
        }
    }
}
