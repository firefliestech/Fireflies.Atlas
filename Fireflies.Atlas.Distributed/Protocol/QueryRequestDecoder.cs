using System.Linq.Expressions;
using Serialize.Linq.Serializers;

namespace Fireflies.Atlas.Distributed.Protocol;

public class QueryRequestDecoder {
    public QueryRequestDescriptor Decode(byte[] content) {
        using var ms = new MemoryStream(content) { Position = 1 };
        using var reader = new BinaryReader(ms);

        var query = new QueryRequestDescriptor {
            Uuid = new Guid(reader.ReadBytes(16)),
            EnableNotifications = reader.ReadBoolean(),
            Expression = new ExpressionSerializer(new JsonSerializer()).DeserializeText(reader.ReadString()) as LambdaExpression
        };

        return query;
    }
}