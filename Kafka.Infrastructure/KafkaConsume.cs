using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Kafka.Sdk.Background;

namespace Kafka.Sdk
{
    public class KafkaConsumeService<TConsumer, TEvent> : ConsumerEvent<TConsumer, TEvent>
    {
        protected virtual string BootstrapServers => throw new NotImplementedException();
        protected virtual string SchemaRegistryUrl => throw new NotImplementedException();
        protected virtual string GroupId => throw new NotImplementedException();

        protected override IConsumer<string, TEvent> Consumer
        {
            get
            {
                var config = new ConsumerConfig
                {
                    BootstrapServers = BootstrapServers,
                    GroupId = GroupId,
                    AutoOffsetReset = AutoOffsetReset.Earliest
                };

                var schemaRegistry = new CachedSchemaRegistryClient(new SchemaRegistryConfig { Url = SchemaRegistryUrl });
                var consumer = new ConsumerBuilder<string, TEvent>(config)
                    .SetValueDeserializer(new AvroDeserializer<TEvent>(schemaRegistry).AsSyncOverAsync())
                    .Build();

                return consumer;
            }
        }

        public KafkaConsumeService(IServiceProvider serviceProvider) : base(serviceProvider)
        {
        }

        protected override Task ExecuteAsync(CancellationToken stoppingToken)
        {
            throw new NotImplementedException();
        }
    }
}
