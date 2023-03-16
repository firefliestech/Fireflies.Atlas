using System.Text.Json;
using System.Text.Json.Serialization;

namespace Fireflies.Atlas.Distributed.Protocol;

public class QueryResponseDecoder {
    public QueryResponseDescriptor Decode(byte[] content, Func<Guid, Type> typeCallback) {
        using var ms = new MemoryStream(content) { Position = 1 };
        using var reader = new BinaryReader(ms);

        var descriptor = new QueryResponseDescriptor {
            Uuid = new Guid(reader.ReadBytes(16)),
            EnableNotifications = reader.ReadBoolean()
        };

        var json = reader.ReadString();
        var type = typeof(IEnumerable<>).MakeGenericType(typeCallback(descriptor.Uuid));
        //descriptor.Documents = (IEnumerable<object>)JsonConvert.DeserializeObject(json, type, new JsonSerializerSettings
        //{
        //    ReferenceLoopHandling = ReferenceLoopHandling.Serialize,
        //    PreserveReferencesHandling = PreserveReferencesHandling.All
        //});


        descriptor.Documents = (IEnumerable<object>)System.Text.Json.JsonSerializer.Deserialize(json, type, new JsonSerializerOptions { ReferenceHandler = ReferenceHandler.Preserve });

        return descriptor;
    }
}