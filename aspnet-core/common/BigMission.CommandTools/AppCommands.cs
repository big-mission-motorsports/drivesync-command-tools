using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Consumer;
using Azure.Messaging.EventHubs.Processor;
using Azure.Messaging.EventHubs.Producer;
using Azure.Storage.Blobs;
using BigMission.CommandTools.Models;
using Newtonsoft.Json;
using NLog;
using System;
using System.Text;
using System.Threading;
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

        private EventProcessorClient processor;
        private Action<Command> commandCallback;
        private bool disposed;

        private readonly string appId;
        private readonly string kafkaConnStr;
        private readonly string groupId;
        private readonly string topic;
        private readonly string blobStorageConnStr;
        private readonly string blobContainer;

        public AppCommands(string appId, string kafkaConnStr, string groupId, string topic, string blobStorageConnStr, string blobContainer, ILogger logger)
        {
            this.appId = appId;
            this.kafkaConnStr = kafkaConnStr;
            this.groupId = groupId;
            this.topic = topic;
            this.blobStorageConnStr = blobStorageConnStr;
            this.blobContainer = blobContainer;
            Logger = logger;
        }

        public void ListenForCommands(Action<Command> commandCallback)
        {
            this.commandCallback = commandCallback ?? throw new InvalidOperationException();

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
            if (eventArgs.Data.Properties.TryGetValue(DESTID, out object destObj))
            {
                if (destObj.ToString() == appId)
                {
                    var json = Encoding.UTF8.GetString(eventArgs.Data.Body.ToArray());
                    var cmd = JsonConvert.DeserializeObject<Command>(json);
                    if (cmd != null)
                    {
                        Logger.Info($"Received command: '{cmd.CommandType}'");
                        ThreadPool.QueueUserWorkItem(ProcessCallback, cmd);
                    }
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
                var cmd = (Command)o;
                commandCallback(cmd);
            }
            catch(Exception ex)
            {
                Logger.Error(ex, "Unable to complete command callback.");
            }
        }

        public async Task SendCommand(Command command)
        {
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
