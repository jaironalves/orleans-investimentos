namespace Orleans.Investimentos.Silo.Abstractions.Graos.Posicao.Models;

[GenerateSerializer]
[Alias("Orleans.Investimentos.Graos.Abstractions.Posicao.Models.PosicaoModel")]
public class PosicaoModel
{
    [Id(0)]
    public string Ativo { get; set; }

    [Id(1)]
    public int Quantidade { get; set; }

    [Id(2)]
    public decimal Preco { get; set; }

    [Id(3)]
    public decimal Valor { get; set; }
}
