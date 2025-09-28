namespace Orleans.Investimentos.Silo.Abstractions.Graos.Ativo.Models;

[GenerateSerializer]
[Alias("Orleans.Investimentos.Graos.Abstractions.Ativo.Models.AtivoPrecoStreamModel")]
public class AtivoPrecoStreamModel
{
    [Id(0)]
    public required string Ativo { get; set; }

    [Id(1)]
    public required decimal Preco { get; set; }    
}
