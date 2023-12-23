using Kafka.Sdk;

namespace Kafka.Api
{
    public class ConsumerPerson : KafkaConsumeService<ConsumerPerson, PersonRecord>
    {
        protected override string GroupId => "person-group";
        protected override string Topic => "my-person-test";
        protected override string BootstrapServers => "localhost:9092";
        protected override string SchemaRegistryUrl => "http://localhost:8081";

        public ConsumerPerson(IServiceProvider serviceProvider) : base(serviceProvider)
        {
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            var message = ConsumeResult?.Value;
            Console.WriteLine($"Id: {message?.Id} Name: {message?.Name}");

            await Task.Delay(100);
        }

        public override async Task OnExceptionAsync(Exception ex)
        {
            Console.WriteLine(ex);

            await Task.CompletedTask;
        }
    }
}
