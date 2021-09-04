using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Consumer;
using Azure.Messaging.EventHubs.Producer;
using NLog;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BigMission.CommandTools
{
    /// <summary>
    /// Helper for listening for configuraiton changes.
    /// </summary>
    public class ConfigurationCommands : IDisposable, IAsyncDisposable
    {
        private const string CMD = "cmd";
        private ILogger Logger { get; }

        private EventHubHelpers ehReader;
        private Task receiveStatus;
        private Func<KeyValuePair<string, string>, Task> commandCallback;
        private string[] commandTypeSubscriptions;

        private bool disposed;
        private readonly string kafkaConnStr;
        private readonly string groupId;
        private readonly string topic;


        public ConfigurationCommands(string kafkaConnStr, string groupId, string topic, ILogger logger)
        {
            this.kafkaConnStr = kafkaConnStr;
            this.groupId = groupId;
            this.topic = topic;
            Logger = logger;
            ehReader = new EventHubHelpers(Logger);
        }


        public void Subscribe(string[] commandTypeSubscriptions, Func<KeyValuePair<string, string>, Task> commandCallback)
        {
            if (commandCallback == null || this.commandTypeSubscriptions != null)
            {
                throw new InvalidOperationException();
            }

            this.commandCallback = commandCallback;
            this.commandTypeSubscriptions = commandTypeSubscriptions;

            // Process changes from stream and cache them here is the service
            receiveStatus = ehReader.ReadEventHubPartitionsAsync(kafkaConnStr, topic, groupId, null, EventPosition.Latest, ReceivedEventCallback);
        }

        private async Task ReceivedEventCallback(PartitionEvent receivedEvent)
        {
            try
            {
                // Check subscriptions
                if (receivedEvent.Data.Properties.TryGetValue(CMD, out object cmdObj))
                {
                    bool subscribed = commandTypeSubscriptions.Contains(cmdObj.ToString());
                    if (subscribed)
                    {
                        Logger?.Info($"Received command: '{cmdObj}'");
                        var value = Encoding.UTF8.GetString(receivedEvent.Data.Body.ToArray());
                        await commandCallback(new KeyValuePair<string, string>(cmdObj.ToString(), value));
                    }
                }
            }
            catch (Exception ex)
            {
                Logger?.Error(ex, "Unable to process event from event hub partition");
            }
        }

        public async Task SendCommandAsync(string command, string value)
        {
            await using var producerClient = new EventHubProducerClient(kafkaConnStr, topic);
            using EventDataBatch eventBatch = await producerClient.CreateBatchAsync();
            var eventData = new EventData(Encoding.UTF8.GetBytes(value));
            eventData.Properties[CMD] = command;
            eventBatch.TryAdd(eventData);
            await producerClient.SendAsync(eventBatch);
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
