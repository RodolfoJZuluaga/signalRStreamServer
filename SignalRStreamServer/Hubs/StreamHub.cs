using Microsoft.AspNetCore.SignalR;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace SignalRStreamServer.Hubs
{
    public class StreamHub : Hub
    {
        private readonly StreamManager _streamManager;

        public StreamHub(StreamManager streamManager)
        {
            _streamManager = streamManager;
        }

        public override Task OnConnectedAsync()
        {
            Clients.Caller.SendAsync("ListStreams", _streamManager.ListStreams());
            return base.OnConnectedAsync();
        }

        public override Task OnDisconnectedAsync(Exception exception)
        {
            return base.OnDisconnectedAsync(exception);
        }

        public IEnumerable<string> ListStreams()
        {
            return _streamManager.ListStreams();
        }

        public IAsyncEnumerable<byte[]> ListenToStream(string streamName, CancellationToken cancellationToken)
        {
            return _streamManager.Subscribe(streamName, cancellationToken);
        }

        public async Task StartStream(string streamName, IAsyncEnumerable<byte[]> streamContent)
        {
            try
            {
                var streamTask = _streamManager.RunStreamAsync(streamName, streamContent);

                await Clients.Others.SendAsync("NewStream", streamName);

                await streamTask;
            }
            finally
            {
                await Clients.Others.SendAsync("RemoveStream", streamName);
            }
        }
    }
}
