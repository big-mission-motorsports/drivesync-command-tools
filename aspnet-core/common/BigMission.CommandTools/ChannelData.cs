using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Producer;
using BigMission.DeviceApp.Shared;
using Newtonsoft.Json;
using System;
using System.Text;
using System.Threading.Tasks;

namespace BigMission.CommandTools
{
    /// <summary>
    /// Sends CAN Bus cannel data.
    /// </summary>
    public class ChannelData : IAsyncDisposable
    {
        private readonly string appId;
        private readonly string kafkaConnStr;
        private readonly string topic;
        private readonly EventHubProducerClient producerClient;

        public ChannelData(string appId, string kafkaConnStr, string topic)
        {
            this.appId = appId;
            this.kafkaConnStr = kafkaConnStr;
            this.topic = topic;
            producerClient = new EventHubProducerClient(kafkaConnStr, topic);
        }


        public async Task SendData(ChannelDataSetDto dataSet)
        {
            using EventDataBatch eventBatch = await producerClient.CreateBatchAsync();
            var data = JsonConvert.SerializeObject(dataSet);
            var eventData = new EventData(Encoding.UTF8.GetBytes(data));
            eventData.Properties["DeviceAppId"] = appId;
            eventBatch.TryAdd(eventData);
            await producerClient.SendAsync(eventBatch);
        }


        public virtual ValueTask DisposeAsync()
        {
            try
            {
                return producerClient.DisposeAsync();
            }
            catch (Exception exception)
            {
                return new ValueTask(Task.FromException(exception));
            }
        }
    }
}
