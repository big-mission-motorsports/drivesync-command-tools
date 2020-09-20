using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Producer;
using BigMission.DeviceApp.Shared;
using Newtonsoft.Json;
using System.Text;
using System.Threading.Tasks;

namespace BigMission.CommandTools
{
    /// <summary>
    /// Sends CAN Bus cannel data.
    /// </summary>
    public class ChannelData
    {
        private readonly string appId;
        private readonly string kafkaConnStr;
        private readonly string topic;

        public ChannelData(string appId, string kafkaConnStr, string topic)
        {
            this.appId = appId;
            this.kafkaConnStr = kafkaConnStr;
            this.topic = topic;
        }


        public async Task SendData(ChannelDataSetDto dataSet)
        {
            await using var producerClient = new EventHubProducerClient(kafkaConnStr, topic);
            using EventDataBatch eventBatch = await producerClient.CreateBatchAsync();
            var data = JsonConvert.SerializeObject(dataSet);
            var eventData = new EventData(Encoding.UTF8.GetBytes(data));
            eventData.Properties["DeviceAppId"] = appId;
            eventBatch.TryAdd(eventData);
            await producerClient.SendAsync(eventBatch);
        }
    }
}
