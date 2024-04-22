using Kafka.Sdk;

namespace Kafka_Console
{
    public class ConsumerProduct : KafkaConsumeService<ConsumerProduct, string>
    {
        public ConsumerProduct(IServiceProvider serviceProvider) : base(serviceProvider)
        {
        }

        protected override string BootstrapServers => "localhost:9092";
        protected override string SchemaRegistryUrl => "http://localhost:8081";
        protected override string Topic => "product-dev";
        protected override string GroupId => "product-consumer-group";

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            Console.WriteLine();
            await Task.CompletedTask;
        }

        public override async Task OnExceptionAsync(Exception ex)
        {
            Console.WriteLine($"{ex.Message}");

            await Task.CompletedTask;
        }

    }
}
