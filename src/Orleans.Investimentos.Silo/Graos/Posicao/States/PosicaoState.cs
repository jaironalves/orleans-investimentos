using Orleans.Streams;

namespace Orleans.Investimentos.Silo.Graos.Posicao.States;

[GenerateSerializer]
[Alias("Orleans.Investimentos.Graos.Posicao.States.PosicaoState")]
public class PosicaoState
{
    [Id(0)]
    public int Quantidade { get; set; }

    [Id(1)]
    public decimal Preco { get; set; }
    
    public decimal Valor { get => Preco * Quantidade; }

    //[Id(2)]
    //public StreamSequenceToken PrecoToken { get; set; }
}
