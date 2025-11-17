using Orleans.Investimentos.Silo.Abstractions.Graos.Ativo;
using Orleans.Investimentos.Silo.Abstractions.Graos.Ativo.Models;
using Orleans.Investimentos.Silo.Abstractions.Graos.Posicao;
using Orleans.Investimentos.Silo.Abstractions.Graos.Posicao.Models;
using Orleans.Investimentos.Silo.Graos.Posicao.States;
using Orleans.Metadata;
using Orleans.Streams;

namespace Orleans.Investimentos.Silo.Graos.Posicao;

class testem : IStreamIdMapper
{
    public IdSpan GetGrainKeyId(GrainBindings grainBindings, StreamId streamId)
    {
        throw new NotImplementedException();
    }    
}

class teste : IStreamNamespacePredicate
{
    public string PredicatePattern => throw new NotImplementedException();

    public bool IsMatch(string streamNamespace)
    {
        throw new NotImplementedException();
    }
}

//[ImplicitStreamSubscription()]
internal class PosicaoGrain(
    [PersistentState("posicao", "Investimentos")]
    IPersistentState<PosicaoState> posicaoState) : Grain
    , IPosicaoGrain
    , IAsyncObserver<AtivoPrecoStreamModel>
{

    private (string Conta, string Ativo, string TipoMercado)? grainKey;

    private (string Conta, string Ativo, string TipoMercado) GrainKey
    { 
        get => grainKey ??= MontarKey();       
    }

    private (string Conta, string Ativo, string TipoMercado) MontarKey()
    {
        var partes = this.GetPrimaryKeyString().Split('-');
        return (partes[0], partes[1], partes[2]);
    }

    public override async Task OnActivateAsync(CancellationToken cancellationToken)
    {
        var ativo = GrainKey.Ativo;

        var precoAtual = await GrainFactory
            .GetGrain<IAtivoGrain>(ativo)
            .ObterPrecoAsync();

        Console.WriteLine($"[{this.GetPrimaryKeyString()}] {ativo} começa em R$ {precoAtual}");

        var precoStream = this
                        .GetStreamProvider("AtivoPrecoStream")
                        .GetStream<AtivoPrecoStreamModel>("AtivoPreco", ativo);

        var allMyHandles =
            await precoStream.GetAllSubscriptionHandles();

        if (allMyHandles.Count > 0)
        {
           foreach (var handle in allMyHandles)
            {
                //var redis = new RedisSequenceToken("1761270917005-0");               
                await handle.ResumeAsync(this);                
            }            
        }
        else
        {
            await precoStream
             .SubscribeAsync(this);
        }        
    }  

    public async Task OnNextAsync(AtivoPrecoStreamModel item, StreamSequenceToken? token = null)
    {
        posicaoState.State.Preco = item.Preco;
        //posicaoState.State.PrecoToken = token;
        await posicaoState.WriteStateAsync();
    }

    public Task OnErrorAsync(Exception ex)
    {
        throw new NotImplementedException();
    }

    public async Task AtualizarQuantidadeAsync(int quantidade)
    {
        posicaoState.State.Quantidade += quantidade;
        await posicaoState.WriteStateAsync();
    }

    public Task<PosicaoModel> ObterAsync()
    {
        var model = new PosicaoModel
        {
            //Conta = GrainKey.Conta,
            Ativo = GrainKey.Ativo,
            //TipoMercado = GrainKey.TipoMercado,
            Preco = posicaoState.State.Preco,
            Quantidade = posicaoState.State.Quantidade,
            Valor = posicaoState.State.Valor
        };

        return Task.FromResult(model);
    }
}
