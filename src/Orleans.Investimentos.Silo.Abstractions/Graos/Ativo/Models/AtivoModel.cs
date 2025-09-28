namespace Orleans.Investimentos.Silo.Abstractions.Graos.Ativo.Models;

[GenerateSerializer]
[Alias("Orleans.Investimentos.Graos.Abstractions.Ativo.Models.AtivoModel")]
public class AtivoModel
{
    [Id(0)]
    public string Ativo { get; set; }

    [Id(1)]
    public decimal Preco { get; set; }
}
