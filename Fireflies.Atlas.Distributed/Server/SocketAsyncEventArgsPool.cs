using System.Collections.Concurrent;
using System.Net.Sockets;

namespace Fireflies.Atlas.Distributed.Server;

public sealed class SocketAsyncEventArgsPool {
    readonly ConcurrentQueue<SocketAsyncEventArgs> _queue;

    public SocketAsyncEventArgsPool(int capacity) {
        _queue = new ConcurrentQueue<SocketAsyncEventArgs>();
    }

    public SocketAsyncEventArgs Pop() {
        SocketAsyncEventArgs args;
        if (_queue.TryDequeue(out args)) {
            return args;
        }

        return null;
    }

    public void Push(SocketAsyncEventArgs item) {
        if (item == null) {
            throw new ArgumentNullException("Items added to a SocketAsyncEventArgsPool cannot be null");
        }

        _queue.Enqueue(item);
    }
}