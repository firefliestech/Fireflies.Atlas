using System.Collections.Concurrent;
using System.Linq.Expressions;
using System.Net;
using System.Net.Sockets;
using Fireflies.Atlas.Distributed.Protocol;
using Fireflies.Atlas.Distributed.Server;

namespace Fireflies.Atlas.Distributed.Client;

public class AtlasClient : SocketClient {
    public ConcurrentDictionary<Guid, QueryRequestDescriptor> _outstanding = new();

    public AtlasClient(IPEndPoint hostEndPoint) : base(hostEndPoint) {
    }

    public override void Connect() {
        while (true) {
            try {
                base.Connect();
                break;
            } catch (SocketException) {
            }
        }
    }

    protected override Task ProcessMessage(byte[] message) {
        var decompressed = Compressor.Decompress(message);
        if (decompressed[0] == 2) {
            // Query response
            var decoder = new QueryResponseDecoder();
            var response = decoder.Decode(decompressed, uuid => _outstanding[uuid].DocumentType);

            var queryRequestDescriptor = _outstanding[response.Uuid];
            queryRequestDescriptor.SetResult(response.Documents);
        }

        return Task.CompletedTask;
    }

    public async Task<T?> QueryFirst<T>(Expression<Func<T, bool>> predicate) {
        var result = await Query(predicate);
        return result.FirstOrDefault();
    }

    public async Task<IEnumerable<TDocument>> Query<TDocument>(Expression<Func<TDocument, bool>>? predicate = null) {
        predicate ??= _ => true;

        var queryRequestDescriptor = new QueryRequestDescriptor<TDocument> {
            Uuid = Guid.NewGuid(),
            EnableNotifications = false,
            Expression = predicate
        };

        var queryEncoder = new QueryRequestEncoder();
        var buffer = await queryEncoder.Encode(queryRequestDescriptor);

        _outstanding[queryRequestDescriptor.Uuid] = queryRequestDescriptor;

        Send(buffer);

        return await queryRequestDescriptor.TaskCompletionSource.Task;
    }
}