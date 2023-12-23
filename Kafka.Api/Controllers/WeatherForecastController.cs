using Kafka.Sdk.Interface;
using Microsoft.AspNetCore.Mvc;

namespace Kafka.Api.Controllers
{
    [ApiController]
    [Route("[controller]")]
    public class WeatherForecastController : ControllerBase
    {
        private static readonly string[] Summaries = new[]
        {
            "Freezing", "Bracing", "Chilly", "Cool", "Mild", "Warm", "Balmy", "Hot", "Sweltering", "Scorching"
        };

        private readonly ILogger<WeatherForecastController> _logger;
        private readonly IKafkaProduce<PersonRecord> _producer;

        public WeatherForecastController(ILogger<WeatherForecastController> logger, IKafkaProduce<PersonRecord> producer)
        {
            _logger = logger;
            _producer = producer;
        }

        [HttpGet(Name = "GetWeatherForecast")]
        public IEnumerable<WeatherForecast> Get()
        {
            return Enumerable.Range(1, 5).Select(index => new WeatherForecast
            {
                Date = DateTime.Now.AddDays(index),
                TemperatureC = Random.Shared.Next(-20, 55),
                Summary = Summaries[Random.Shared.Next(Summaries.Length)]
            })
            .ToArray();
        }

        [HttpPost]
        public async Task PublicarKafka()
        {
            var msg = new PersonRecord { Id = 10, Name = "Fernando Ferrari" };
            await _producer.Produce(msg);
        }
    }
}
