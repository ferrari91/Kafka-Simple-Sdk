using Confluent.Kafka;
using Kafka.Sdk.Background;
using Microsoft.Extensions.Options;

namespace Kafka.Sdk
{
    public class KafkaConsumeService<TConsumer, TEvent> : ConsumerEvent<TConsumer, TEvent>
    {

        public KafkaConsumeService(IServiceProvider serviceProvider) : base(serviceProvider)
        {
        }

        protected virtual string GroupId => "_groupId";

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

                var consumer = new ConsumerBuilder<string, TEvent>(config)
                    .SetValueDeserializer(new CustomDeserializer<TEvent>())
                    .Build();

                return consumer;
            }
        }
    }
}
