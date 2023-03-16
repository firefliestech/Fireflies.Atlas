using Serialize.Linq.Serializers;

namespace Fireflies.Atlas.Distributed.Protocol;

public class QueryRequestEncoder {
    public async Task<byte[]> Encode(QueryRequestDescriptor requestDescriptor) {
        using var ms = new MemoryStream();
        await using var writer = new BinaryWriter(ms);
        writer.Write((byte)1); // Query request
        writer.Write(requestDescriptor.Uuid.ToByteArray());
        writer.Write(requestDescriptor.EnableNotifications);

        var serializer = new ExpressionSerializer(new JsonSerializer());
        var query = serializer.SerializeText(requestDescriptor.Expression);
        writer.Write(query);
        writer.Flush();

        return Compressor.Compress(ms);
    }
}