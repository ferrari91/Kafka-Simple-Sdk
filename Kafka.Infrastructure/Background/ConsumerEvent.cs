using Confluent.Kafka;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Kafka.Sdk.Background
{
    public abstract class ConsumerEvent<TConsumer, TEvent> : BackgroundService
    {
        private readonly IServiceProvider _serviceProvider;

        public ConsumerEvent(IServiceProvider serviceProvider)
        {
            _serviceProvider = serviceProvider;
            ConsumeResult = new ConsumeResult<string, TEvent>();
        }

        protected Task? TaskExecuting;
        protected ConsumeResult<string, TEvent> ConsumeResult { get; private set; }
        protected virtual IConsumer<string, TEvent> Consumer => throw new NotImplementedException();
        protected virtual string Topic => throw new NotImplementedException();

        private async Task InternalExecuteAsync(CancellationToken ctx)
        {
            await Task.Yield();

            using (var consumer = Consumer)
            {
                consumer.Subscribe(Topic);
                while (true)
                {
                    try
                    {
                        var consumeResult = consumer.Consume();
                        if (consumeResult != null)
                        {
                            ConsumeResult = consumeResult;
                            using (var scope = _serviceProvider.CreateScope())
                            {
                                await ExecuteAsync(ctx);
                            }
                        }
                        consumer.Commit(consumeResult);
                    }
                    catch (Exception ex)
                    {
                        await OnExceptionAsync(ex);
                    }
                    finally
                    {
                    }
                }
            }
        }

        public override Task StartAsync(CancellationToken cancellationToken)
        {
            TaskExecuting = InternalExecuteAsync(cancellationToken);
            while (TaskExecuting.IsCompleted)
                return TaskExecuting;

            return Task.CompletedTask;
        }

        public virtual Task OnExceptionAsync(Exception ex) => throw new NotImplementedException();
    }
}
