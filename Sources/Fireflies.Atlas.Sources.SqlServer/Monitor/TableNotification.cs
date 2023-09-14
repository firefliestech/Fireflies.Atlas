using System.Text.Json;
using System.Text.Json.Nodes;

namespace Fireflies.Atlas.Sources.SqlServer.Monitor;

public abstract class TableNotification {
    public abstract void Process(JsonObject jsonDocument);
}

public class TableNotification<TDocument> : TableNotification {
    private readonly JsonSerializerOptions _serializerOptions;

    public event DocumentInsert<TDocument> Inserted;
    public event DocumentUpdated<TDocument> Updated;
    public event DocumentDelete<TDocument> Deleted;

    public TableNotification() {
        _serializerOptions = new JsonSerializerOptions { PropertyNameCaseInsensitive = true, Converters = { new XmlStringToEnumConverter(), new AutoStringToNumberConverter(), new XmlStringToBooleanConverter() } };
    }

    public override void Process(JsonObject jsonDocument) {
        var insertedRow = jsonDocument["inserted"]?["row"];
        var deletedRow = jsonDocument["deleted"]?["row"];

        if(insertedRow != null) {
            var document = insertedRow.Deserialize<TDocument>(_serializerOptions)!;

            if(deletedRow != null) {
                Updated?.Invoke(document, new Lazy<TDocument>(deletedRow.Deserialize<TDocument>(_serializerOptions)!));
            } else {
                Inserted?.Invoke(document);
            }
        } else if(deletedRow != null) {
            var document = deletedRow.Deserialize<TDocument>(_serializerOptions);
            Deleted?.Invoke(document);
        }
    }
}