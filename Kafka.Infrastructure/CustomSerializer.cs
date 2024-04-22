using Confluent.Kafka;
using System.Text.Json;

namespace Kafka.Sdk
{
    public class CustomDeserializer<TEvent> : IDeserializer<TEvent>
    {
        public TEvent Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
        {
            if (isNull)
                return default;

            var json = System.Text.Encoding.UTF8.GetString(data);
            return JsonSerializer.Deserialize<TEvent>(json);
        }
    }

    public class CustomSerializer<TEvent> : ISerializer<TEvent>
    {
        public byte[] Serialize(TEvent data, SerializationContext context)
        {
            if (data == null)
                return null;

            var serializedValue = JsonSerializer.Serialize(data);
            return System.Text.Encoding.UTF8.GetBytes(serializedValue);
        }
    }
}
