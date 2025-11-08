using Microsoft.Extensions.DependencyInjection;
using Moq;
using Orleans.Investimentos.Silo.Graos.Ativo;
using Orleans.Investimentos.Silo.Graos.Ativo.States;

namespace Orleans.Investimentos.UnitTests
{
    public class UnitTest1
    {
        [Fact]
        public async Task Test1()
        {
            var mock = new Mock<IPersistentState<AtivoState>>();
            //var grainContextMock = new Mock<IGrainContext>();

            //var reminderRegistryMock = new Mock<IGrainReminderRegistry>();

            var grainContextMock = new Mock<IGrainContext>();
            //ctxMock.Setup(x => x.ReminderRegistry).Returns(reminderRegistryMock.Object);
            //grainContextMock.Setup(x => x.GrainId).Returns(GrainId.Create("MeuGrain", Guid.NewGuid()));
            grainContextMock.Setup(x => x.ActivationServices)
                  .Returns(new ServiceCollection().BuildServiceProvider());

            var grain = new AtivoGrain(mock.Object)
            {
               // GrainContext = grainContextMock.Object
            };

            await grain.AtualizarPrecoAsync(24);
        }
    }
}