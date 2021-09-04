using Azure.Messaging.EventHubs.Consumer;
using Newtonsoft.Json;
using NLog;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace BigMission.CommandTools
{
    public class EventHubHelpers
    {
        private Func<PartitionEvent, Task> callback;
        private ILogger Logger { get; }
        private readonly CancellationTokenSource cancellationToken = new CancellationTokenSource();


        public EventHubHelpers(ILogger logger)
        {
            Logger = logger;
        }


        public async Task ReadEventHubPartitionsAsync(string ehConnection, string ehName, string consumerGroup,
            string[] partitionIdsFilter, EventPosition eventPosition, Func<PartitionEvent, Task> callback)
        {
            this.callback = callback;

            await using var consumer = new EventHubConsumerClient(consumerGroup, ehConnection, ehName);

            var activePartitions = await consumer.GetPartitionIdsAsync();

            var partitionConsumers = new List<Task>();
            foreach (var partitionId in activePartitions)
            {
                if (partitionIdsFilter == null || partitionIdsFilter.Contains(partitionId))
                {
                    Task read = ReadPartition(consumer, partitionId, eventPosition);
                    partitionConsumers.Add(read);
                }
                else
                {
                    Logger?.Warn($"Partition {partitionId} does not exist, skipping");
                }
            }

            Task.WaitAll(partitionConsumers.ToArray());
        }

        private async Task ReadPartition(EventHubConsumerClient consumer, string partitionId, EventPosition eventPosition)
        {
            await foreach (PartitionEvent receivedEvent in consumer.ReadEventsFromPartitionAsync(partitionId, eventPosition, cancellationToken.Token))
            {
                try
                {
                    await callback(receivedEvent);
                }
                catch (Exception ex)
                {
                    Logger?.Error(ex, "Unable to process event from event hub partition");
                }
            }
        }

        public void CancelProcessing()
        {
            cancellationToken.Cancel();
        }

        public static string[] GetPartitionFilter(string idStr)
        {
            if (!string.IsNullOrWhiteSpace(idStr))
            {
                return idStr.Split(',');
            }
            return null;
        }
    }
}
