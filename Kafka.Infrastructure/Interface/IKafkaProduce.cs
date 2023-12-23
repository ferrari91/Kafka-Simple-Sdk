namespace Kafka.Sdk.Interface
{
    public interface IKafkaProduce<TEvent>
    {
        Task Produce(TEvent value);
    }
}
