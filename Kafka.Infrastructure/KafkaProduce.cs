using Confluent.Kafka;

namespace Kafka.Sdk
{
    public abstract class KafkaProduce<TEvent> : IDisposable
    {
        protected virtual string BootstrapServers => throw new NotImplementedException();
        protected virtual string Topic => throw new NotImplementedException();

        private IProducer<string, TEvent> Producer
        {
            get
            {
                var config = new ProducerConfig { BootstrapServers = BootstrapServers };
                return new ProducerBuilder<string, TEvent>(config)
                    .SetValueSerializer(new CustomSerializer<TEvent>())
                    .Build();
            }
        }

        public void Dispose()
        {
            Producer.Dispose();
        }

        protected async Task Produce(TEvent value)
        {
            var message = new Message<string, TEvent>
            {
                Key = Guid.NewGuid().ToString(),
                Value = value
            };

            await Producer.ProduceAsync(Topic, message);
        }
    }
}
