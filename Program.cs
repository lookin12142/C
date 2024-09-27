using Confluent.Kafka;
using System;
using System.Threading.Tasks;

class Program
{
    public static async Task Main(string[] args)
    {
        var config = new ProducerConfig
        {
            BootstrapServers = "161.132.40.126:29092" 
        };

        using (var producer = new ProducerBuilder<Null, string>(config).Build())
        {
            while (true)
            {
                try
                {
                    var message = $"Mensaje enviado a las {DateTime.Now}";
                    var dr = await producer.ProduceAsync("hola", new Message<Null, string> { Value = message });
                    Console.WriteLine($"Mensaje '{dr.Value}' entregado a '{dr.TopicPartitionOffset}'");
                }
                catch (ProduceException<Null, string> e)
                {
                    Console.WriteLine($"Fallo en la entrega: {e.Error.Reason}");
                }

                await Task.Delay(5000);
            }
        }
    }
}
