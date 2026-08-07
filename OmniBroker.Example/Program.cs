using System.Text;
using OmniBroker;
using OmniBroker.Interfaces;
using OmniBroker.RabbitMQ;
using Microsoft.AspNetCore.Mvc;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.
// Learn more about configuring Swagger/OpenAPI at https://aka.ms/aspnetcore/swashbuckle
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

builder.Services.AddBroker(options =>
{
    options.SetupName = "Example";
    options.UseRabbitMq(new OmniBroker.RabbitMQ.ServiceSetup.RabbitMQSettings
    {
        HostName = "192.168.219.45",
        UserName = "guest",
        Password = "guest",
    });
    options.AddProducerFor<ExampleMessage>();
    options.AddConsumerFor<ExampleMessage>((ExampleMessage m) => { Console.WriteLine($"RabbitMQ received: {m.Text}"); return Task.FromResult(true); });
    options.AddConsumerFor<ExampleMessage>(MessageHandler.HandleMyMessage);
    options.AddRpcCaller<ExampleMessage2, ExampleMessage3>();
    options.AddRpcReceiver<ExampleMessage2, ExampleMessage3>(async (ExampleMessage2 message) =>
    {
        //await Task.Delay(1000 * 60 + 1000);
        return await Task.FromResult(new ExampleMessage3 { Text = new string([.. message.Text.Reverse()]) });
    });
});

//builder.Services.AddBroker(options =>
//{
//    options.SetupName = "Example";
//    options.UseKafka(new KafkaSettings { Hosts = "localhost:9092" });
//    options.AddProducerFor<ExampleMessage>();
//    options.AddConsumerFor<ExampleMessage>((ExampleMessage m) => { Console.WriteLine($"Kafka received: {m.Text}"); return Task.FromResult(true); });
//});

var app = builder.Build();

app.UseBrokers();

// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}

app.UseHttpsRedirection();

app.MapGet("/testBroker/send/{text}",
    async (
        [FromRoute] string text,
        IProducer<ExampleMessage> producer
    ) =>
    {
        await producer.Publish(new ExampleMessage { Text = text });
    });

app.MapGet("/testBroker/call/{text}",
    async (
        [FromRoute] string text,
        IRpcCaller<ExampleMessage2, ExampleMessage3> caller
    ) =>
    {
        try
        {
            return (await caller.Call(new ExampleMessage2 { Text = text })).Text;
        }
        catch (Exception ex)
        {
            return "error";
        }
    });

app.Run();

public class ExampleMessage : IMessage
{
    public string CorrelationId { get; set; }
    public byte[] Body { get => Encoding.UTF8.GetBytes(Text); set => Text = Encoding.UTF8.GetString(value); }
    public string Text { get; set; } = "Test";
    public string Tag { get; set; }
}

public class ExampleMessage1 : IMessage
{
    public string CorrelationId { get; set; }
    public byte[] Body { get => Encoding.UTF8.GetBytes(Text); set => Text = Encoding.UTF8.GetString(value); }
    public string Text { get; set; } = "Test";
    public string Tag { get; set; }
}

public class ExampleMessage2 : IMessage
{
    public string CorrelationId { get; set; }
    public byte[] Body { get => Encoding.UTF8.GetBytes(Text); set => Text = Encoding.UTF8.GetString(value); }
    public string Text { get; set; } = "Test";
    public string Tag { get; set; }
}

public class ExampleMessage3 : IMessage
{
    public string CorrelationId { get; set; }
    public byte[] Body { get => Encoding.UTF8.GetBytes(Text); set => Text = Encoding.UTF8.GetString(value); }
    public string Text { get; set; } = "Test";
    public string Tag { get; set; }
}

public class MessageHandler
{
    public static async Task<bool> HandleMyMessage(IHostEnvironment env, ExampleMessage message)
    {
        Console.WriteLine($"Message is {message.Text}");
        return true;
    }
}

