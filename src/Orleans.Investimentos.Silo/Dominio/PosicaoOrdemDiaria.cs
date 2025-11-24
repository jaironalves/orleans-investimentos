namespace Orleans.Investimentos.Silo.Dominio;

public class PosicaoOrdemDiaria
{
    public DateOnly Dia { get; set; }

    // Valor inicial do dia (herdado do dia anterior)
    public int ExecutadoInicial { get; set; }

    // Valor inicial do dia (herdado do dia anterior)
    public int PendenteInicial { get; set; }

    // Quanto realmente executou naquele dia
    public int Executado { get; set; }

    // Pendente final após os eventos daquele dia
    public int Pendente { get; set; }

    // Dados brutos recebidos do evento (total acumulado)
    //public int ExecutadoAcumuladoNoEvento { get; set; }
    //public int PendenteNoEvento { get; set; }
}
