using Orleans.Investimentos.Silo.Dominio;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Text.Json;

namespace Orleans.Investimentos.UnitTests.Dominio;

public class PosicaoAtivoTests
{
    public readonly JsonSerializerOptions jsonOptions = new()
    {
        WriteIndented = true,
        Converters =
            {
                new System.Text.Json.Serialization.JsonStringEnumConverter()
            }
    };

    [Fact]
    public void ProcessarComTodosEventos()
    {
        // Arrange
        var posicao = new PosicaoAtivo
        {
            Ativo = "PETR4",
            Quantidade = 100
        };

        var dia1 = new DateTime(2025, 11, 20);
        var dia2 = new DateTime(2025, 11, 21);
        var dia3 = new DateTime(2025, 11, 22);

        var eventos = new List<PosicaoOrdemEvento>
        {
            // Evento de novo (dia 1) - ordem criada com total 50, sem execuções
            new PosicaoOrdemEvento
            {
                OrdemId = "ORD1",
                Ativo = "PETR4",
                Tipo = PosicaoOrdemEventoTipo.Adicao,
                DataHoraTransacao = dia1,
                QuantidadeTotal = 100,
                QuantidadeExecutada = 0,
                QuantidadeNegocio = 0
            },

            // Execução (dia 2) - cumulativo 20
            new PosicaoOrdemEvento
            {
                OrdemId = "ORD1",
                Ativo = "PETR4",
                Tipo = PosicaoOrdemEventoTipo.Execucao,
                DataHoraTransacao = dia2,
                QuantidadeTotal = 100,
                QuantidadeExecutada = 20,
                QuantidadeNegocio = 20
            },

            // Execução (dia 3) - cumulativo 35
            new PosicaoOrdemEvento
            {
                OrdemId = "ORD1",
                Ativo = "PETR4",
                Tipo = PosicaoOrdemEventoTipo.Execucao,
                DataHoraTransacao = dia3,
                QuantidadeTotal = 100,
                QuantidadeExecutada = 55,
                QuantidadeNegocio = 35
            },

            // Alteração (dia 3) - alteração do total para 120
            new PosicaoOrdemEvento
            {
                OrdemId = "ORD1",
                Ativo = "PETR4",
                Tipo = PosicaoOrdemEventoTipo.Alteracao,
                DataHoraTransacao = dia3,
                QuantidadeTotal = 120,
                QuantidadeExecutada = 55,
                QuantidadeNegocio = 0
            }
        };

        // Act
        posicao.ProcessarOrdensEventos(eventos);

        var json = JsonSerializer.Serialize(posicao.Ordens, jsonOptions);
        Console.WriteLine(json);

        // Assert
        Assert.Single(posicao.Ordens);
        Assert.True(posicao.Ordens.ContainsKey("ORD1"));

        var ordem = posicao.Ordens["ORD1"];

        // Ordem acumulados
        Assert.Equal(120, ordem.QuantidadeTotal);
        Assert.Equal(55, ordem.QuantidadeExecutada);
        Assert.Equal(65, ordem.QuantidadePendente);

        //// Diário deve conter entradas para os três dias
        //var d1 = DateOnly.FromDateTime(dia1);
        //var d2 = DateOnly.FromDateTime(dia2);
        //var d3 = DateOnly.FromDateTime(dia3);

        //Assert.True(ordem.Diario.ContainsKey(d1));
        //Assert.True(ordem.Diario.ContainsKey(d2));
        //Assert.True(ordem.Diario.ContainsKey(d3));

        //var diario1 = ordem.Diario[d1];
        //var diario2 = ordem.Diario[d2];
        //var diario3 = ordem.Diario[d3];

        //// Valida valores conforme a implementação atual
        //Assert.Equal(50, diario1.PendenteInicial);
        //Assert.Equal(0, diario1.Executado);
        //Assert.Equal(50, diario1.Pendente);

        //Assert.Equal(50, diario2.PendenteInicial);
        //// Observação: a implementação atual calcula Executado como ExecutadoInicial - QuantidadeExecutada,
        //// logo o valor pode ficar negativo; aqui validamos o comportamento atual.
        //Assert.Equal(-20, diario2.Executado);
        //Assert.Equal(30, diario2.Pendente);

        //Assert.Equal(30, diario3.PendenteInicial);
        //Assert.Equal(-15, diario3.Executado);
        //Assert.Equal(15, diario3.Pendente);

        //// A posição inicial do ativo não é alterada pela rotina atual de processamento de ordens
        //Assert.Equal(100, posicao.Quantidade);
    }

    [Fact]
    public void ProcessarComDuasExecucoesMesmoDia()
    {
        // Arrange
        var posicao = new PosicaoAtivo
        {
            Ativo = "PETR4",
            Quantidade = 100
        };

        var dia1 = new DateTime(2025, 11, 20);
        var dia2 = new DateTime(2025, 11, 21);
        var dia3 = new DateTime(2025, 11, 22);

        var eventos = new List<PosicaoOrdemEvento>
        {
            // Evento de novo (dia 1) - ordem criada com total 100, sem execuções
            new PosicaoOrdemEvento
            {
                OrdemId = "ORD1",
                Ativo = "PETR4",
                Tipo = PosicaoOrdemEventoTipo.Adicao,
                DataHoraTransacao = dia1,
                QuantidadeTotal = 100,
                QuantidadeExecutada = 0,
                QuantidadeNegocio = 0
            },

            // Execução (dia 2) - cumulativo 20
            new PosicaoOrdemEvento
            {
                OrdemId = "ORD1",
                Ativo = "PETR4",
                Tipo = PosicaoOrdemEventoTipo.Execucao,
                DataHoraTransacao = dia2,
                QuantidadeTotal = 100,
                QuantidadeExecutada = 20,
                QuantidadeNegocio = 20
            },

            // Execução (dia 3) - cumulativo 55
            new PosicaoOrdemEvento
            {
                OrdemId = "ORD1",
                Ativo = "PETR4",
                Tipo = PosicaoOrdemEventoTipo.Execucao,
                DataHoraTransacao = dia3,
                QuantidadeTotal = 100,
                QuantidadeExecutada = 55,
                QuantidadeNegocio = 35
            },

            // Execução (dia 3) - cumulativo 70
            new PosicaoOrdemEvento
            {
                OrdemId = "ORD1",
                Ativo = "PETR4",
                Tipo = PosicaoOrdemEventoTipo.Execucao,
                DataHoraTransacao = dia3,
                QuantidadeTotal = 100,
                QuantidadeExecutada = 70,
                QuantidadeNegocio = 15
            }
        };

        // Act
        posicao.ProcessarOrdensEventos(eventos);

        var json = JsonSerializer.Serialize(posicao.Ordens, jsonOptions);
        Console.WriteLine(json);

        // Assert
        Assert.Single(posicao.Ordens);
        Assert.True(posicao.Ordens.ContainsKey("ORD1"));

        var ordem = posicao.Ordens["ORD1"];

        // Ordem acumulados
        Assert.Equal(100, ordem.QuantidadeTotal);
        Assert.Equal(70, ordem.QuantidadeExecutada);
        Assert.Equal(30, ordem.QuantidadePendente);
      
    }

    [Fact]
    public void ProcessarComReplacePrimeiro()
    {
        // Arrange
        var dia1 = new DateTime(2025, 11, 20);

        var posicao = new PosicaoAtivo
        {
            Ativo = "PETR4",
            Quantidade = 100,
            Dia = DateOnly.FromDateTime(dia1)
        };
        
        //var dia2 = new DateTime(2025, 11, 21);
        //var dia3 = new DateTime(2025, 11, 22);

        var eventos = new List<PosicaoOrdemEvento>
        {
            // Evento de novo (dia 1) - ordem alterada com total 150
            new PosicaoOrdemEvento
            {
                OrdemId = "ORD1",
                Ativo = "PETR4",
                Tipo = PosicaoOrdemEventoTipo.Alteracao,
                DataHoraTransacao = dia1,
                QuantidadeTotal = 150,
                QuantidadeExecutada = 30,
                QuantidadeNegocio = 0
            },

            // Execução (dia 1) - cumulativo 20
            new PosicaoOrdemEvento
            {
                OrdemId = "ORD1",
                Ativo = "PETR4",
                Tipo = PosicaoOrdemEventoTipo.Execucao,
                DataHoraTransacao = dia1,
                QuantidadeTotal = 150,
                QuantidadeExecutada = 50,
                QuantidadeNegocio = 20
            }
        };

        // Act
        posicao.ProcessarOrdensEventos(eventos);
                
        // Assert
        Assert.Single(posicao.Ordens);
        Assert.True(posicao.Ordens.ContainsKey("ORD1"));

        var ordem = posicao.Ordens["ORD1"];

        // Ordem acumulados
        Assert.Equal(150, ordem.QuantidadeTotal);
        Assert.Equal(50, ordem.QuantidadeExecutada);
        Assert.Equal(100, ordem.QuantidadePendente);

    }

    [Fact]
    public void Processar02()
    {
        // Arrange
        var posicao = new PosicaoAtivo
        {
            Ativo = "PETR4",
            Quantidade = 100
        };

        var dia1 = new DateTime(2025, 11, 20);
        var dia2 = new DateTime(2025, 11, 21);
        var dia3 = new DateTime(2025, 11, 22);

        var eventos = new List<PosicaoOrdemEvento>
        {
            //// Evento de novo (dia 1) - ordem criada com total 50, sem execuções
            //new PosicaoOrdemEvento
            //{
            //    OrdemId = "ORD1",
            //    Ativo = "PETR4",
            //    Tipo = PosicaoOrdemEventoTipo.Adicao,
            //    DataHoraTransacao = dia1,
            //    QuantidadeTotal = 100,
            //    QuantidadeExecutada = 0
            //},

            // Execução (dia 2) - cumulativo 20
            new PosicaoOrdemEvento
            {
                OrdemId = "ORD1",
                Ativo = "PETR4",
                Tipo = PosicaoOrdemEventoTipo.Execucao,
                DataHoraTransacao = dia2,
                QuantidadeTotal = 100,
                QuantidadeExecutada = 20,
                QuantidadeNegocio = 20
            },

            // Execução (dia 3) - cumulativo 35
            new PosicaoOrdemEvento
            {
                OrdemId = "ORD1",
                Ativo = "PETR4",
                Tipo = PosicaoOrdemEventoTipo.Execucao,
                DataHoraTransacao = dia3,
                QuantidadeTotal = 100,
                QuantidadeExecutada = 55,
                QuantidadeNegocio = 35
            }
        };

        // Act
        posicao.ProcessarOrdensEventos(eventos);

        var json = JsonSerializer.Serialize(posicao.Ordens, jsonOptions);
        Console.WriteLine(json);

        // Assert
        Assert.Single(posicao.Ordens);
        Assert.True(posicao.Ordens.ContainsKey("ORD1"));

        var ordem = posicao.Ordens["ORD1"];

        // Ordem acumulados
        Assert.Equal(100, ordem.QuantidadeTotal);
        Assert.Equal(55, ordem.QuantidadeExecutada);
        Assert.Equal(45, ordem.QuantidadePendente);        
    }


    [Fact]
    public void Processar03()
    {
        // Arrange
        var posicao = new PosicaoAtivo
        {
            Ativo = "PETR4",
            Quantidade = 100
        };

        var dia1 = new DateTime(2025, 11, 20);
        var dia2 = new DateTime(2025, 11, 21);
        var dia3 = new DateTime(2025, 11, 22);

        var eventos = new List<PosicaoOrdemEvento>
        {
            //// Evento de novo (dia 1) - ordem criada com total 50, sem execuções
            //new PosicaoOrdemEvento
            //{
            //    OrdemId = "ORD1",
            //    Ativo = "PETR4",
            //    Tipo = PosicaoOrdemEventoTipo.Adicao,
            //    DataHoraTransacao = dia1,
            //    QuantidadeTotal = 100,
            //    QuantidadeExecutada = 0
            //},

            // Execução (dia 2) - cumulativo 20
            //new PosicaoOrdemEvento
            //{
            //    OrdemId = "ORD1",
            //    Ativo = "PETR4",
            //    Tipo = PosicaoOrdemEventoTipo.Execucao,
            //    DataHoraTransacao = dia2,
            //    QuantidadeTotal = 100,
            //    QuantidadeExecutada = 20
            //},

            // Execução (dia 3) - cumulativo 35
            new PosicaoOrdemEvento
            {
                OrdemId = "ORD1",
                Ativo = "PETR4",
                Tipo = PosicaoOrdemEventoTipo.Execucao,
                DataHoraTransacao = dia3,
                QuantidadeTotal = 100,
                QuantidadeExecutada = 55,
                QuantidadeNegocio = 35
            }
        };

        // Act
        posicao.ProcessarOrdensEventos(eventos);

        

        var json = JsonSerializer.Serialize(posicao.Ordens, jsonOptions);
        Console.WriteLine(json);

        // Assert
        Assert.Single(posicao.Ordens);
        Assert.True(posicao.Ordens.ContainsKey("ORD1"));

        var ordem = posicao.Ordens["ORD1"];

        // Ordem acumulados
        Assert.Equal(100, ordem.QuantidadeTotal);
        Assert.Equal(55, ordem.QuantidadeExecutada);
        Assert.Equal(45, ordem.QuantidadePendente);
    }
}

