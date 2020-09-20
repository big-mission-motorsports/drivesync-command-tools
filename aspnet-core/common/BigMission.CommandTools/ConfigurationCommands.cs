using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Consumer;
using Azure.Messaging.EventHubs.Processor;
using Azure.Messaging.EventHubs.Producer;
using Azure.Storage.Blobs;
using NLog;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
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

        private EventProcessorClient processor;
        private Action<KeyValuePair<string, string>> commandCallback;
        private string[] commandTypeSubscriptions;

        private bool disposed;
        private readonly string kafkaConnStr;
        private readonly string groupId;
        private readonly string topic;
        private readonly string blobStorageConnStr;
        private readonly string blobContainer;


        public ConfigurationCommands(string kafkaConnStr, string groupId, string topic, string blobStorageConnStr, string blobContainer, ILogger logger)
        {
            this.kafkaConnStr = kafkaConnStr;
            this.groupId = groupId;
            this.topic = topic;
            this.blobStorageConnStr = blobStorageConnStr;
            this.blobContainer = blobContainer;
            Logger = logger;
        }


        public void Subscribe(string[] commandTypeSubscriptions, Action<KeyValuePair<string, string>> commandCallback)
        {
            if (commandCallback == null || this.commandTypeSubscriptions != null)
            {
                throw new InvalidOperationException();
            }

            this.commandCallback = commandCallback;
            this.commandTypeSubscriptions = commandTypeSubscriptions;

            // Process changes from stream and cache them here is the service
            var storageClient = new BlobContainerClient(blobStorageConnStr, blobContainer);
            processor = new EventProcessorClient(storageClient, groupId, kafkaConnStr, topic);
            processor.ProcessEventAsync += CommandProcessEventHandler;
            processor.ProcessErrorAsync += CommandProcessErrorHandler;
            processor.PartitionInitializingAsync += Processor_PartitionInitializingAsync;
            processor.StartProcessing();
        }

        private Task Processor_PartitionInitializingAsync(PartitionInitializingEventArgs arg)
        {
            arg.DefaultStartingPosition = EventPosition.Latest;
            return Task.CompletedTask;
        }

        private async Task CommandProcessEventHandler(ProcessEventArgs eventArgs)
        {
            // Check subscriptions
            if (eventArgs.Data.Properties.TryGetValue(CMD, out object cmdObj))
            {
                bool subscribed = commandTypeSubscriptions.Contains(cmdObj.ToString());
                if (subscribed)
                {
                    Logger.Info($"Received command: '{cmdObj}'");
                    var value = Encoding.UTF8.GetString(eventArgs.Data.Body.ToArray());
                    ThreadPool.QueueUserWorkItem(ProcessCallback, new KeyValuePair<string, string>(cmdObj.ToString(), value));
                }
            }
            // Update checkpoint in the blob storage so that the app receives only new events the next time it's run
            await eventArgs.UpdateCheckpointAsync(eventArgs.CancellationToken);
        }

        private Task CommandProcessErrorHandler(ProcessErrorEventArgs eventArgs)
        {
            // Write details about the error to the console window
            Logger.Error(eventArgs.Exception, $"\tPartition '{ eventArgs.PartitionId}': an unhandled exception was encountered.");
            return Task.CompletedTask;
        }

        private void ProcessCallback(object o)
        {
            try
            {
                var cmd = (KeyValuePair<string, string>)o;
                commandCallback(cmd);
            }
            catch (Exception ex)
            {
                Logger.Error(ex, "Unable to complete command callback.");
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
                processor.StopProcessing();
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
