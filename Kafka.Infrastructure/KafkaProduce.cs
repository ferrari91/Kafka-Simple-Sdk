using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;

namespace Kafka.Sdk
{
    public abstract class KafkaProduce<TEvent> : IDisposable
    {
        protected virtual string BootstrapServers => throw new NotImplementedException();
        protected virtual string SchemaRegistryUrl => throw new NotImplementedException();
        protected virtual string Topic => throw new NotImplementedException();

        private IProducer<string, TEvent> Producer
        {
            get
            {
                var config = new ProducerConfig { BootstrapServers = BootstrapServers };

                var schemaRegistry = new CachedSchemaRegistryClient(new SchemaRegistryConfig { Url = SchemaRegistryUrl });
                var producer = new ProducerBuilder<string, TEvent>(config)
                    .SetValueSerializer(new AvroSerializer<TEvent>(schemaRegistry))
                    .Build();

                return producer;
            }
        }

        public void Dispose()
        {
            Producer.Dispose();
        }

        public async Task Produce(TEvent value)
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
