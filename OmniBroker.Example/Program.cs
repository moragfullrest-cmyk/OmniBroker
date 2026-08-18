using System.Text;
using Microsoft.AspNetCore.Mvc;
using OmniBroker;
using OmniBroker.Infrastructure;
using OmniBroker.Interfaces;
using OmniBroker.Kafka.ServiceSetup;
using OmniBroker.RabbitMQ;
using OmniBroker.RabbitMQ.ServiceSetup;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.
// Learn more about configuring Swagger/OpenAPI at https://aka.ms/aspnetcore/swashbuckle
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

var rabbitSection = builder.Configuration.GetSection("Brokers:RabbitMQ");
var kafkaSection = builder.Configuration.GetSection("Brokers:Kafka");

if (rabbitSection.GetValue("Enabled", true))
{
    var rabbitSettings = rabbitSection.Get<RabbitMQSettings>()
        ?? throw new InvalidOperationException("Brokers:RabbitMQ is enabled but settings could not be bound from configuration.");

    builder.Services.AddBroker(options =>
    {
        options.SetupName = "Example";
        options.UseRabbitMq(rabbitSettings);
        options.AddProducerFor<ExampleMessage>();
        options.AddConsumerFor<ExampleMessage>((ExampleMessage m) => { Console.WriteLine($"RabbitMQ received: {m.Text}"); return Task.FromResult(HandleResult.Ack); });
        options.AddConsumerFor<ExampleMessage>(MessageHandler.HandleMyMessage);
        options.AddRpcCaller<ExampleMessage2, ExampleMessage3>();
        options.AddRpcReceiver<ExampleMessage2, ExampleMessage3>(async (ExampleMessage2 message) =>
        {
            //await Task.Delay(1000 * 60 + 1000);
            return await Task.FromResult(new ExampleMessage3 { Text = new string([.. message.Text.Reverse()]) });
        });
    });
}

if (kafkaSection.GetValue("Enabled", true))
{
    var kafkaSettings = kafkaSection.Get<KafkaSettings>()
        ?? throw new InvalidOperationException("Brokers:Kafka is enabled but settings could not be bound from configuration.");

    builder.Services.AddBroker(options =>
    {
        options.SetupName = "Example";
        options.UseKafka(kafkaSettings);
        options.AddProducerFor<ExampleMessage>();
        options.AddConsumerFor<ExampleMessage>((ExampleMessage m) => { Console.WriteLine($"Kafka received: {m.Text}"); return Task.FromResult(HandleResult.Ack); });
    });
}

var app = builder.Build();

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
    public static Task<HandleResult> HandleMyMessage(IHostEnvironment env, ExampleMessage message)
    {
        Console.WriteLine($"Message is {message.Text}");
        return Task.FromResult(HandleResult.Ack);
    }
}
