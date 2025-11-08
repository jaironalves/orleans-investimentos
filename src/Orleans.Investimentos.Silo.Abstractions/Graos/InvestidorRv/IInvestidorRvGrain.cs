namespace Orleans.Investimentos.Silo.Abstractions.Graos.InvestidorRv;

[Alias(nameof(IInvestidorRvGrain))]
public interface IInvestidorRvGrain : IGrainWithStringKey
{
    [Alias("ObterAsync")]
    Task<object> ObterAsync();

    [Alias("SalvarAsync")]
    Task SalvarAsync();
}
