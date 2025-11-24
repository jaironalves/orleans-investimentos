namespace Orleans.Investimentos.Silo.Dominio;

public enum PosicaoOrdemLado
{
    Compra,
    Venda
}

public enum PosicaoOrdemCiclo
{
    Adicao,
    AguardandoNegocio,
    Encerrada
}

public class PosicaoOrdem
{
    public required string OrdemId { get; set; }
    public required PosicaoOrdemLado Lado { get; set; }
    public required int QuantidadeTotal { get; set; }
    public required PosicaoOrdemCiclo Ciclo { get; set; } = PosicaoOrdemCiclo.Adicao;
    public int QuantidadeExecutada { get; set; }
    public int QuantidadePendente => QuantidadeTotal - QuantidadeExecutada;
    public int QuantidadeCancelada { get; set; }
    public int QuantidadeExpirada { get; set; }

    public Dictionary<DateOnly, PosicaoOrdemDiaria> Diario { get; set; } = [];

    internal void ProcessarEvento(PosicaoOrdemEvento evento)
    {
        var dia = DateOnly.FromDateTime(evento.DataHoraTransacao);
        if (!Diario.TryGetValue(dia, out var posicaoOrdemDiaria))
        {
            posicaoOrdemDiaria = new PosicaoOrdemDiaria
            {
                Dia = dia,
                PendenteInicial = QuantidadePendente,
                ExecutadoInicial = Ciclo switch
                {
                    PosicaoOrdemCiclo.Adicao => evento.QuantidadeExecutada - evento.QuantidadeNegocio,
                    PosicaoOrdemCiclo.AguardandoNegocio => QuantidadeExecutada,
                    _ => QuantidadeExecutada
                }
            };
            Diario[dia] = posicaoOrdemDiaria;
        }

        switch (evento.Tipo)
        {
            case PosicaoOrdemEventoTipo.Adicao:
                QuantidadeTotal = evento.QuantidadeTotal;
                QuantidadeExecutada = evento.QuantidadeExecutada;
                Ciclo = PosicaoOrdemCiclo.AguardandoNegocio;
                break;
            case PosicaoOrdemEventoTipo.Execucao:
                QuantidadeTotal = evento.QuantidadeTotal;
                QuantidadeExecutada = evento.QuantidadeExecutada;
                Ciclo = PosicaoOrdemCiclo.AguardandoNegocio;
                break;
            case PosicaoOrdemEventoTipo.Alteracao:
                QuantidadeTotal = evento.QuantidadeTotal;
                Ciclo = PosicaoOrdemCiclo.AguardandoNegocio;
                break;
            case PosicaoOrdemEventoTipo.Cancelamento:
                QuantidadeCancelada += evento.QuantidadeExecutada;
                Ciclo = PosicaoOrdemCiclo.Encerrada;
                break;
        }

        posicaoOrdemDiaria.Pendente = QuantidadePendente;
        posicaoOrdemDiaria.Executado = QuantidadeExecutada - posicaoOrdemDiaria.ExecutadoInicial;
    }



}
