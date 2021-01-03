using Microsoft.AspNetCore.SignalR;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace SignalRStreamServer
{
    public class StreamManager
    {
        private readonly ConcurrentDictionary<string, StreamHolder> _streams = new ConcurrentDictionary<string, StreamHolder>();
        private long _globalClientId;

        public IEnumerable<string> ListStreams()
        {
            return _streams.Keys;
        }

        public async Task RunStreamAsync(string streamName, IAsyncEnumerable<byte[]> stream)
        {
            var streamHolder = new StreamHolder() { Source = stream };

            // Add before yielding
            // This fixes a race where we tell clients a new stream arrives before adding the stream
            _streams.TryAdd(streamName, streamHolder);

            await Task.Yield();

            try
            {
                await foreach (var item in stream)
                {
                    foreach (var viewer in streamHolder.Viewers)
                    {
                        try
                        {
                            await viewer.Value.Writer.WriteAsync(item);
                        }
                        catch { }
                    }
                }
            }
            finally
            {
                RemoveStream(streamName);
            }
        }

        public void RemoveStream(string streamName)
        {
            _streams.TryRemove(streamName, out var streamHolder);
            foreach (var viewer in streamHolder.Viewers)
            {
                viewer.Value.Writer.TryComplete();
            }
        }

        public IAsyncEnumerable<byte[]> Subscribe(string streamName, CancellationToken cancellationToken)
        {
            if (!_streams.TryGetValue(streamName, out var source))
            {
                throw new HubException("stream doesn't exist");
            }

            var id = Interlocked.Increment(ref _globalClientId);

            var channel = Channel.CreateBounded<byte[]>(options: new BoundedChannelOptions(2)
            {
                FullMode = BoundedChannelFullMode.DropOldest
            });

            source.Viewers.TryAdd(id, channel);

            // Register for client closing stream, this token will always fire (handled by SignalR)
            cancellationToken.Register(() =>
            {
                source.Viewers.TryRemove(id, out _);
            });

            return channel.Reader.ReadAllAsync();
        }


        private class StreamHolder
        {
            public IAsyncEnumerable<byte[]> Source;
            public ConcurrentDictionary<long, Channel<byte[]>> Viewers = new ConcurrentDictionary<long, Channel<byte[]>>();
        }
    }
}
