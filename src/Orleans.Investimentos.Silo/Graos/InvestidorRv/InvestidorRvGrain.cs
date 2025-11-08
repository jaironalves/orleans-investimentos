using Orleans.Investimentos.Silo.Abstractions.Graos.InvestidorRv;
using Orleans.Investimentos.Silo.Graos.Base;
using Orleans.Investimentos.Silo.Graos.Base.Investidor.State;
using Orleans.Investimentos.Silo.Graos.InvestidorRv.State;
using Orleans.Runtime;

namespace Orleans.Investimentos.Silo.Graos.InvestidorRv;

class InvestidorRvGrain([PersistentState("investidorRv", "Investimentos")] IPersistentState<InvestidorRvState> persistentState) 
    : GrainStringKey, IInvestidorRvGrain
{
    public Task<object> ObterAsync()
    {
        return Task.FromResult<object>(persistentState.State);
    }

    public async Task SalvarAsync()
    {   
        persistentState.State.Nome = "Investidor RV Exemplo";
        persistentState.State.Posicoes["GOL4"] = new InvestidorPosicaoRvState
        {
            Ativo = "GOL4",
            Quantidade = 100            
        };

        await persistentState.WriteStateAsync();
    }
}
