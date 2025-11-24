using System.Text.Json;

namespace Orleans.Investimentos.Silo.Dominio
{
    public class PosicaoAtivo
    {
        public readonly JsonSerializerOptions jsonOptions = new()
        {
            WriteIndented = true,
            Converters =
            {
                new System.Text.Json.Serialization.JsonStringEnumConverter()
            }
        };

        public string Ativo { get; set; }
        public DateOnly Dia { get; set; }
        public int Quantidade { get; set; }
        public int QuantidadeCompraPendente { get; set; }
        public int QuantidadeCompraExecutada { get; set; }
        public int QuantidadeVendaPendente { get; set; }
        public int QuantidadeVendaExecutada { get; set; }
        public Dictionary<string, PosicaoOrdem> Ordens { get; set; } = [];

        public override string ToString()
            => $"Ativo={Ativo} Quantidade={Quantidade} Ordens.Count={Ordens.Count}";

        public void ProcessarOrdensEventos(IEnumerable<PosicaoOrdemEvento> eventos)
        {
            var eventosAtivo = eventos.Where(e => e.Ativo.Equals(Ativo));
            foreach (var evento in eventosAtivo)
            {
                if (!Ordens.TryGetValue(evento.OrdemId, out var posicaoOrdem))
                {
                    posicaoOrdem = new PosicaoOrdem
                    {
                        OrdemId = evento.OrdemId,
                        Lado = evento.Lado,
                        QuantidadeTotal = evento.QuantidadeTotal,
                        QuantidadeExecutada = evento.QuantidadeExecutada,
                        Ciclo = PosicaoOrdemCiclo.Adicao,
                    };
                    Ordens[evento.OrdemId] = posicaoOrdem;
                }
                posicaoOrdem.ProcessarEvento(evento);

                Console.WriteLine("PosicaoOrdem atualizado: {0}", evento);
                var jsonOrdem = JsonSerializer.Serialize(posicaoOrdem, jsonOptions);
                Console.WriteLine(jsonOrdem);
            }

            AtualizarQuantidadesDiarias();
            var jsonAtivo = JsonSerializer.Serialize(this, jsonOptions);
            Console.WriteLine(jsonAtivo);
        }

        private void AtualizarQuantidadesDiarias()
        {
            // Reset daily aggregates
            QuantidadeCompraPendente = 0;
            QuantidadeCompraExecutada = 0;
            QuantidadeVendaPendente = 0;
            QuantidadeVendaExecutada = 0;

            if (Ordens == null || Ordens.Count == 0)
                return;

            foreach (var ordem in Ordens.Values)
            {
                if (ordem.Diario == null)
                    continue;

                if (!ordem.Diario.TryGetValue(Dia, out var diario))
                    continue;
                
                var executadoDiario = Math.Max(0, diario.Executado);
                var pendenteDiario = Math.Max(0, diario.Pendente);

                if (ordem.Lado == PosicaoOrdemLado.Compra)
                {
                    QuantidadeCompraExecutada += executadoDiario;
                    QuantidadeCompraPendente += pendenteDiario;
                }
                else
                {
                    QuantidadeVendaExecutada += executadoDiario;
                    QuantidadeVendaPendente += pendenteDiario;
                }
            }
        }
    }
}
