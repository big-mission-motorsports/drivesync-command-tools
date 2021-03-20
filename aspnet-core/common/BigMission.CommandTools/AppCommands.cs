using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Consumer;
using Azure.Messaging.EventHubs.Producer;
using BigMission.CommandTools.Models;
using Newtonsoft.Json;
using NLog;
using System;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace BigMission.CommandTools
{
    /// <summary>
    /// Support methods for sending Commands to other services.
    /// </summary>
    public class AppCommands : IDisposable
    {
        const string DESTID = "DestId";
        private ILogger Logger { get; }

        private Action<Command> commandCallback;
        private readonly EventHubHelpers ehReader;

        private volatile bool disposed;

        private readonly string appId;
        private readonly string kafkaConnStr;
        private readonly string groupId;

        public AppCommands(string appId, string kafkaConnStr, ILogger logger)
        {
            this.appId = appId;
            this.kafkaConnStr = kafkaConnStr;
            Logger = logger;
            ehReader = new EventHubHelpers(logger);
            groupId = EventHubConsumerClient.DefaultConsumerGroupName;
        }

        public async Task ListenForCommandsAsync(Action<Command> commandCallback, string topic)
        {
            this.commandCallback = commandCallback ?? throw new InvalidOperationException();

            // There should be a guid at the end of the command channel for the destination app
            // This regex checks for a guid at the end of the topic name
            var guidRegex = new Regex("([0-9A-Fa-f]{8}[-][0-9A-Fa-f]{4}[-][0-9A-Fa-f]{4}[-][0-9A-Fa-f]{4}[-][0-9A-Fa-f]{12})$");
            var hasGuid = guidRegex.IsMatch(topic);
            if (!hasGuid)
            {
                topic += "-" + appId;
            }

            while (!disposed)
            {
                try
                {
                    await ehReader.ReadEventHubPartitionsAsync(kafkaConnStr, topic, groupId, null, EventPosition.Latest, ReceivedEventCallback);
                    break;
                }
                catch (Exception ex)
                {
                    Logger.Error(ex, "Unable to connect, retrying.");
                    await Task.Delay(2000);
                }
            }
        }

        private void ReceivedEventCallback(PartitionEvent receivedEvent)
        {
            try
            {
                if (receivedEvent.Data.Properties.TryGetValue(DESTID, out object destObj))
                {
                    if (destObj.ToString() == appId)
                    {
                        Logger?.Trace($"Received command on partition {receivedEvent.Partition.PartitionId}");
                        var json = Encoding.UTF8.GetString(receivedEvent.Data.Body.ToArray());
                        var cmd = JsonConvert.DeserializeObject<Command>(json);
                        if (cmd != null)
                        {
                            commandCallback(cmd);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Logger?.Error(ex, "Unable to process event from event hub partition");
            }
        }


        public async Task SendCommand(Command command, string topic, string destinationGuid = null)
        {
            if (!string.IsNullOrWhiteSpace(destinationGuid))
            {
                topic += "-" + destinationGuid;
            }

            await using var producerClient = new EventHubProducerClient(kafkaConnStr, topic);
            using EventDataBatch eventBatch = await producerClient.CreateBatchAsync();
            var data = JsonConvert.SerializeObject(command);
            var eventData = new EventData(Encoding.UTF8.GetBytes(data));
            eventData.Properties[DESTID] = command.DestinationId;
            eventBatch.TryAdd(eventData);
            await producerClient.SendAsync(eventBatch);
        }


        /// <summary>
        /// Packs specificed object to base 64 into the commands data object.
        /// </summary>
        /// <param name="data"></param>
        /// <param name="cmd"></param>
        public static void EncodeCommandData(object data, Command cmd)
        {
            var json = JsonConvert.SerializeObject(data);
            var jsonBuff = Encoding.UTF8.GetBytes(json);
            var base64Config = Convert.ToBase64String(jsonBuff);
            cmd.Data = base64Config;
        }

        public static T DecodeCommandData<T>(Command cmd)
        {
            var jsonBuff = Convert.FromBase64String(cmd.Data);
            var jsonStr = Encoding.UTF8.GetString(jsonBuff);
            var decoded = JsonConvert.DeserializeObject<T>(jsonStr);
            return decoded;
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (disposed)
                return;

            if (disposing)
            {
                ehReader.CancelProcessing();
            }

            disposed = true;
        }

        public virtual ValueTask DisposeAsync()
        {
            try
            {
                Dispose();
                return default;
            }
            catch (Exception exception)
            {
                return new ValueTask(Task.FromException(exception));
            }
        }
    }
}
