using System.Text.Json;
using System.Text.Json.Serialization;
using Fireflies.Logging.Abstractions;

namespace Fireflies.Atlas.Distributed.Protocol;

public class QueryResponseEncoder {
    private readonly IFirefliesLogger _logger;

    public QueryResponseEncoder(IFirefliesLoggerFactory loggerFactory) {
        _logger = loggerFactory.GetLogger<QueryResponseEncoder>();
    }

    public byte[] Encode(QueryResponseDescriptor queryResponse) {
        var startedAt = DateTimeOffset.UtcNow;

        using var ms = new MemoryStream();
        using var writer = new BinaryWriter(ms);

        writer.Write((byte)2); // Query response
        writer.Write(queryResponse.Uuid.ToByteArray());
        writer.Write(queryResponse.EnableNotifications);

        var json = JsonSerializer.Serialize(queryResponse.Documents, new JsonSerializerOptions { ReferenceHandler = ReferenceHandler.Preserve });
        writer.Write(json);
       
        _logger.Debug(() => $"Encoding took {(DateTimeOffset.UtcNow - startedAt).TotalMilliseconds}ms");

        var result = Compressor.Compress(ms.ToArray());

        return result;
    }
}