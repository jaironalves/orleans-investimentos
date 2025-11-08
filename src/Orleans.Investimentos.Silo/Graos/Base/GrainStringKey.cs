using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Orleans.Investimentos.Silo.Graos.Base
{
    internal class GrainStringKey : Grain, IGrainWithStringKey
    {
        internal GrainStringKey() { }

        internal GrainStringKey(IGrainContext grainContext) : base(grainContext)
        {
            
        }
    }
}
