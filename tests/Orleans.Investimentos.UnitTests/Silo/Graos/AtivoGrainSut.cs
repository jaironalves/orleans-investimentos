using Orleans.Investimentos.Silo.Graos.Ativo;
using Orleans.Investimentos.Silo.Graos.Ativo.States;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Orleans.Investimentos.UnitTests.Silo.Graos
{
    internal class AtivoGrainSut : AtivoGrain
    {
        public AtivoGrainSut(IPersistentState<AtivoState> persistentState)
            : base(persistentState)
        {
            
        }

        
    }
}
