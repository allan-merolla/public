using System;
using System.Text;
using System.Threading.Tasks;
using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Producer;

namespace ConsoleApp1
{
    class Program
    {
        private const string CONN = "Endpoint=sb://xxxx.servicebus.windows.net/;SharedAccessKeyName=access;SharedAccessKey=xxxxx;EntityPath=xxxx";

        static async Task Main(string[] args)
        {
            await ProduceMessage();

        }

        static async Task ProduceMessage()
        {
            EventHubProducerClient client = new EventHubProducerClient(CONN, "xxxx");

            using EventDataBatch batch = await client.CreateBatchAsync();
            batch.TryAdd(new EventData(System.Text.Encoding.UTF8.GetBytes("test")));
            await client.SendAsync(batch);

        }
    }
}