using BigMission.CommandTools.Models;
using Microsoft.AspNetCore.SignalR.Client;
using Newtonsoft.Json;
using NLog;
using System;
using System.Text;
using System.Threading.Tasks;

namespace BigMission.CommandTools
{
    /// <summary>
    /// Support methods for sending Commands to other services.
    /// </summary>
    public class AppCommands : IDisposable
    {
        private ILogger Logger { get; }

        private volatile bool disposed;
        public HubConnection HubConnection { get; }
        private bool reconnectActive;


        public AppCommands(Guid appId, string apiKey, string url, ILogger logger)
        {
            Logger = logger;

            HubConnection = new HubConnectionBuilder()
                .WithUrl(url, option =>
                {
                    option.AccessTokenProvider = async () =>
                    {
                        var token = KeyUtilities.EncodeToken(appId, apiKey);
                        return await Task.FromResult(token);
                    };
                }).Build();

            HubConnection.Closed += HubConnection_Closed;
        }

        private async Task HubConnection_Closed(Exception arg)
        {
            if (!reconnectActive)
            {
                reconnectActive = true;
                try
                {
                    while (HubConnection.State == HubConnectionState.Disconnected)
                    {
                        await Task.Delay(TimeSpan.FromSeconds(5));
                        Logger?.Debug("Attempting to reconnect to service hub.");
                        await TryConnectAsync();
                    }
                }
                finally
                {
                    reconnectActive = false;
                }
            }
        }

        private async Task<bool> TryConnectAsync()
        {
            if (HubConnection.State == HubConnectionState.Disconnected)
            {
                try
                {
                    await HubConnection.StartAsync();
                    Logger?.Debug("Connected to service hub");
                }
                catch (Exception ex)
                {
                    Logger?.Error(ex, "Error connecting to service hub.");

                    // Start reconnect sequence
                    await HubConnection_Closed(null).ConfigureAwait(false);
                }
            }

            return HubConnection.State == HubConnectionState.Connected;
        }

        public HubConnection GetHubAsync()
        {
            //await TryConnectAsync();
            return HubConnection;
        }

        public async Task ListenForCommandsAsync(Func<Command, Task> commandCallback)
        {
            await TryConnectAsync();
            HubConnection.On("ReceiveCommandV1", async (Command command) =>
            {
                Logger?.Debug($"RX {command.CommandType}");
                await commandCallback(command);
            });
        }

        public async Task SendCommandAsync(Command command, Guid destinationGuid)
        {
            await TryConnectAsync();
            await HubConnection.SendAsync("SendCommandV1", command, destinationGuid);
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

            if (HubConnection != null)
            {
                HubConnection.DisposeAsync().Wait();
            }

            disposed = true;
        }

        public virtual async ValueTask DisposeAsync()
        {
            Dispose();

            if (HubConnection != null)
            {
                await HubConnection.DisposeAsync();
            }
        }
    }
}
