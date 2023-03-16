using System.Linq.Expressions;
using System.Net.Sockets;
using System.Reflection;
using Fireflies.Atlas.Distributed.Protocol;

namespace Fireflies.Atlas.Distributed.Server;

public class AtlasServer : SocketListener {
    private readonly int _port;
    private readonly Core.Atlas _atlas;

    private readonly MethodInfo _executeDocumentMethod;

    public AtlasServer(Atlas.Core.Atlas atlas) : base(1000, 65000) {
        _atlas = atlas;
        _executeDocumentMethod = typeof(AtlasServer).GetMethod(nameof(ExecuteQuery), BindingFlags.Instance | BindingFlags.NonPublic)!;
    }

    protected override void ProcessMessage(byte[] messageData, AsyncUserToken token, SocketAsyncEventArgs e) {
        var decompressed = Compressor.Decompress(messageData);
        if (decompressed[0] == 1) {
            // Query
            HandleQuery(decompressed, token);
        }
    }

    private void HandleQuery(byte[] decompressed, AsyncUserToken token) {
        var decoder = new QueryRequestDecoder();
        var query = decoder.Decode(decompressed);

        _executeDocumentMethod.MakeGenericMethod(query.DocumentType).Invoke(this, new object[] { query.Expression, query, token });
    }

    private async Task ExecuteQuery<TDocument>(Expression<Func<TDocument, bool>> predicate, QueryRequestDescriptor requestDescriptor, AsyncUserToken token) where TDocument : new() {
        var documents = await _atlas.GetDocuments(predicate);
        var response = new QueryResponseDescriptor { Uuid = requestDescriptor.Uuid, EnableNotifications = requestDescriptor.EnableNotifications, Documents = documents.Cast<object>() };
        var encoder = new QueryResponseEncoder(_atlas.LoggerFactory);
        var encoded = encoder.Encode(response);
        Send(encoded, token);
    }
}

public class QueryRequestDescriptor<TDocument> : QueryRequestDescriptor {
    public TaskCompletionSource<IEnumerable<TDocument>> TaskCompletionSource = new();

    internal override void SetResult(IEnumerable<object> result) {
        TaskCompletionSource.SetResult(result.Cast<TDocument>());
    }
}