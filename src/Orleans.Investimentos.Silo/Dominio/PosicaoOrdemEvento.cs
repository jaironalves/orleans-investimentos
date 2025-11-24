namespace Orleans.Investimentos.Silo.Dominio
{
    public enum PosicaoOrdemEventoTipo
    {
        Adicao,
        Execucao,
        Alteracao,
        Cancelamento,
        Rejeicao
    }    

    public class PosicaoOrdemEvento
    {        
        public string OrdemId { get; set; }
        public string ExecucaoId { get; set; }
        //public string ClOrdId { get; set; }

        public string Ativo { get; set; }

        public PosicaoOrdemLado Lado { get; set; }

        public PosicaoOrdemEventoTipo Tipo { get; set; }

        public DateTime DataHoraTransacao { get; set; }
        public DateOnly Dia => DateOnly.FromDateTime(DataHoraTransacao);

        public int QuantidadeTotal { get; set; }
        public int QuantidadeExecutada { get; set; }
        public int QuantidadeRestante { get; set; }

        public int QuantidadeNegocio { get; set; }

        public override string ToString()
            => $"OrdemId={OrdemId} ExecucaoId={ExecucaoId} Dia={Dia} Ativo={Ativo} Tipo={Tipo}";
    }
}
