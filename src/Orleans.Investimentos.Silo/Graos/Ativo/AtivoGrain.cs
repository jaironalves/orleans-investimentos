using Orleans.Investimentos.Silo.Abstractions.Graos.Ativo;
using Orleans.Investimentos.Silo.Abstractions.Graos.Ativo.Models;
using Orleans.Investimentos.Silo.Graos.Ativo.States;
using Orleans.Investimentos.Silo.Graos.Base;
using Orleans.Streams;

namespace Orleans.Investimentos.Silo.Graos.Ativo;

internal class AtivoGrain(
    [PersistentState("ativo", "Investimentos")] 
    IPersistentState<AtivoState> ativoState) : GrainStringKey, IAtivoGrain
{
   
   
    private IAsyncStream<AtivoPrecoStreamModel> _precoStream;

    public override Task OnActivateAsync(CancellationToken cancellationToken)
    {
        _precoStream = this
                        .GetStreamProvider("AtivoPrecoStream")
                        .GetStream<AtivoPrecoStreamModel>("AtivoPreco", this.GetPrimaryKeyString());

        return Task.CompletedTask;
    }

    public async Task AtualizarPrecoAsync(decimal preco)
    {
        var conext = GrainContext;
        var grainbase = (this as Grain);
        var contextbase = grainbase.GrainContext;
        //await this.RegisterOrUpdateReminder("teste", TimeSpan.FromSeconds(3000), TimeSpan.FromSeconds(10));

        ativoState.State.Preco = preco;
        await ativoState.WriteStateAsync();

        await _precoStream.OnNextAsync(new AtivoPrecoStreamModel
        {
            Ativo = this.GetPrimaryKeyString(),
            Preco = ativoState.State.Preco
        });
    }

    public Task<decimal> ObterPrecoAsync() => Task.FromResult(ativoState.State.Preco);

    public Task<AtivoModel> ObterAsync()
    {
        var model = new AtivoModel
        {
            Ativo = this.GetPrimaryKeyString(),
            Preco = ativoState.State.Preco
        };

        return Task.FromResult(model);
    }

    //    public async Task BecomeConsumer(Guid streamId, string streamNamespace, string providerToUse)
    //    {
    ////        logger.LogInformation("BecomeConsumer");
    //  //      consumerObserver = new SampleConsumerObserver<int>(this);
    //        IStreamProvider streamProvider = this.GetStreamProvider(providerToUse);
    //        consumer = streamProvider.GetStream<int>(streamNamespace, streamId);
    //        consumerHandle = await consumer.SubscribeAsync(consumerObserver);
    //    }

    //    public override Task OnActivateAsync(CancellationToken cancellationToken)
    //    {
    //        IStreamProvider streamProvider = this.GetStreamProvider("providerToUse");
    //        consumer = streamProvider.GetStream<int>(streamNamespace, streamId);


    //        return base.OnActivateAsync(cancellationToken);
    //    }
}
