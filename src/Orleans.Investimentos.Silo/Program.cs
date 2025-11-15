
using Microsoft.AspNetCore.Http.HttpResults;
using Microsoft.AspNetCore.Mvc;
using Orleans.Investimentos.ServiceDefaults;
using Orleans.Investimentos.Silo.Abstractions.Graos.Ativo;
using Orleans.Investimentos.Silo.Abstractions.Graos.InvestidorRv;
using Orleans.Investimentos.Silo.Abstractions.Graos.Posicao;
using Orleans.Investimentos.Silo.Abstractions.Graos.Worker;
using Orleans.Investimentos.Silo.Extensions;
using Orleans.Investimentos.Silo.Models;

var builder = WebApplication.CreateBuilder(args);

builder.AddServiceDefaults();

builder.AddOrleans();


// Add services to the container.
//builder.Services.AddAuthorization();

// Learn more about configuring Swagger/OpenAPI at https://aka.ms/aspnetcore/swashbuckle
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

var app = builder.Build();

// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}

app.UseHttpsRedirection();

//app.UseAuthorization();

app.MapDefaultEndpoints();

app.Map("/dashboard", x => x.UseOrleansDashboard());


app.MapPost("/worker-notificar", async (IGrainFactory grainFactory) =>
{
    var itens = Enumerable.Range(10000, 15000).Select(i => i.ToString()).ToList();

    await Parallel.ForEachAsync(itens, async (item, ct) =>
    {
        var gerenciadorWorkerGrain = grainFactory.GetGrain<IGerenciadorWorkerGrain>(item);
        await gerenciadorWorkerGrain.NotificarAsync(DateOnly.FromDateTime(DateTime.Now));
    });
    
    return Results.Ok();
})
.WithName("PostWorkerNotificar")
.WithOpenApi();

app.MapPost("/worker-agendar", async (IGrainFactory grainFactory) =>
{
    var itens = Enumerable.Range(1, 30).Select(i => i.ToString()).ToList();

    await Parallel.ForEachAsync(itens, async (item, ct) =>
    {
        var gerenciadorWorkerGrain = grainFactory.GetGrain<IParticaoWorkerGrain>(item);
        await gerenciadorWorkerGrain.AgendarAsync();
    });

    return Results.Ok();
})
.WithName("PostWorkerAgendar")
.WithOpenApi();

app.MapPost("/investidor-rv", async (IGrainFactory grainFactory) =>
{    
    var investidorGrain = grainFactory.GetGrain<IInvestidorRvGrain>("12345");
    //await investidorGrain.SalvarAsync();
    var model = await investidorGrain.ObterAsync();

    return Results.Ok(model);
})
.WithName("PostInvestidorRv")
.WithOpenApi();

app.MapPost("/posicao", async (IGrainFactory grainFactory, [FromBody]AtivarPosicaoPost request) =>
{
    var key = $"{request.Conta}-{request.Ativo}-VIS";
    var posicaoGrain = grainFactory.GetGrain<IPosicaoGrain>(key);
    await posicaoGrain.AtualizarQuantidadeAsync(request.Quantidade);
    var model = await posicaoGrain.ObterAsync();

    return Results.Ok(model);    
})
.WithName("PostPosicao")
.WithOpenApi();

app.MapGet("/posicao", async (IGrainFactory grainFactory, string conta, string ativo) =>
{
    var key = $"{conta}-{ativo}-VIS";
    var posicaoGrain = grainFactory.GetGrain<IPosicaoGrain>(key);
    var model = await posicaoGrain.ObterAsync();

    return Results.Ok(model);
})
.WithName("GetPosicao")
.WithOpenApi();



app.MapPost("/ativo", async (IGrainFactory grainFactory, [FromBody]AtualizarPrecoPost preco) =>
{
    var ativoGrain = grainFactory.GetGrain<IAtivoGrain>(preco.Ativo);
    await ativoGrain.AtualizarPrecoAsync(preco.Preco);
    var model = await ativoGrain.ObterAsync();

    return Results.Ok(model);
})
.WithName("PostAtivo")
.WithOpenApi();

app.MapGet("/ativo", async (IGrainFactory grainFactory, string ativo) =>
{
    var ativoGrain = grainFactory.GetGrain<IAtivoGrain>(ativo);    
    var model = await ativoGrain.ObterAsync();

    return Results.Ok(model);
})
.WithName("GetAtivo")
.WithOpenApi();



//app.MapGet("/weatherforecast", (HttpContext httpContext) =>
//{
//    var forecast = Enumerable.Range(1, 5).Select(index =>
//        new WeatherForecast
//        {
//            Date = DateOnly.FromDateTime(DateTime.Now.AddDays(index)),
//            TemperatureC = Random.Shared.Next(-20, 55),
//            Summary = summaries[Random.Shared.Next(summaries.Length)]
//        })
//        .ToArray();
//    return forecast;
//})
//.WithName("GetWeatherForecast")
//.WithOpenApi();

app.Run();
