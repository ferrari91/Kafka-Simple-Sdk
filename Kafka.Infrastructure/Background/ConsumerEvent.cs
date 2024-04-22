using Confluent.Kafka;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Polly;

namespace Kafka.Sdk.Background
{
    public abstract class ConsumerEvent<TConsumer, TEvent> : BackgroundService
    {
        private readonly IServiceScope _scope;
        private readonly IServiceProvider _serviceProvider;
        private int _retryCount = 0;

        public ConsumerEvent(IServiceProvider serviceProvider)
        {
            _scope = serviceProvider.CreateScope();
            _serviceProvider = serviceProvider;

            ConsumeResult = new ConsumeResult<string, TEvent>();
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken) => await Policy
            .Handle<Exception>()
            .RetryForeverAsync(_ =>
            {
                Task.Delay(TimeSpan.FromSeconds(3)).Wait();
            })
            .ExecuteAsync(async () =>
            {
                using (var consumer = Consumer)
                {
                    consumer.Subscribe(Topic);
                    while (!stoppingToken.IsCancellationRequested)
                    {
                        var message = consumer.Consume();
                        if (message is not null)
                        {
                            await OnEventAsync(message, stoppingToken);
                            consumer.Commit(message);
                        }
                    }
                }
            });

        private async Task OnEventAsync(ConsumeResult<string, TEvent> message, CancellationToken cancellationToken)
        {
            try
            {
                await OnConsumerAsync(message.Value, cancellationToken);
            }
            catch (Exception ex)
            {
                await OnExceptionAsync(ex, message.Value, cancellationToken);
                
                if (_retryCount < Attempts)
                    _retryCount++;
                else
                {
                    await MoveToDeadLetterQueueAsync(message.Value, cancellationToken);
                    _retryCount = 0;
                }
            }
        }
        private async Task MoveToDeadLetterQueueAsync(TEvent message, CancellationToken cancellationToken)
        {
            var config = new ProducerConfig { BootstrapServers = BootstrapServers };
            
            using (var producer = new ProducerBuilder<string, TEvent>(config)
                 .SetValueSerializer(new CustomSerializer<TEvent>())
                 .Build())
            {
                var deadTopic = new Message<string, TEvent>
                {
                    Key = Guid.NewGuid().ToString(),
                    Value = message
                };

                await producer.ProduceAsync(Topic + "_dead", deadTopic);
            }
        }


        protected ConsumeResult<string, TEvent> ConsumeResult { get; private set; }
        protected virtual IConsumer<string, TEvent> Consumer => throw new NotImplementedException();
        protected virtual string Topic => "_topic";
        protected virtual string BootstrapServers => "_bootstrapServers";
        protected virtual int Attempts => 3;
       

        public virtual async Task OnConsumerAsync(TEvent message, CancellationToken cancellationToken) => await Task.CompletedTask;
        public virtual async Task OnExceptionAsync(Exception ex, TEvent message, CancellationToken cancellationToken) => await Task.CompletedTask;
    }
}
