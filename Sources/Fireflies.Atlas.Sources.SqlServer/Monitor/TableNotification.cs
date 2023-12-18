using System.Text.Json;
using System.Text.Json.Nodes;
using Fireflies.Atlas.Core.Helpers;

namespace Fireflies.Atlas.Sources.SqlServer.Monitor;

public abstract class TableNotification {
    public abstract void Process(JsonObject jsonDocument);
}

public class TableNotification<TDocument> : TableNotification where TDocument : class {
    private readonly JsonSerializerOptions _serializerOptions = new() { PropertyNameCaseInsensitive = true, Converters = { new XmlStringToEnumConverter(), new AutoStringToNumberConverter(), new XmlStringToBooleanConverter() } };

    public event DocumentInsert<TDocument>? Inserted;
    public event DocumentUpdated<TDocument>? Updated;
    public event DocumentDelete<TDocument>? Deleted;

    public override void Process(JsonObject jsonDocument) {
        var insertedRow = jsonDocument["inserted"]?["row"];
        var deletedRow = jsonDocument["deleted"]?["row"];

        var multiplesInsertedRows = insertedRow != null && insertedRow.GetValueKind() == JsonValueKind.Array;
        var multipleDeletedRows = deletedRow != null && deletedRow.GetValueKind() == JsonValueKind.Array;
        var multipleRowsAffected = multiplesInsertedRows || multipleDeletedRows;
        if(multipleRowsAffected) {
            var insertedDocuments = GetDocuments(insertedRow, multiplesInsertedRows);
            var deletedDocuments = GetDocuments(deletedRow, multipleDeletedRows);

            ProcessMultipleRows(insertedDocuments, deletedDocuments);
        } else {
            var insertedDocument = insertedRow != null ? insertedRow.Deserialize<TDocument>(_serializerOptions)! : null;
            var deletedDocument = deletedRow != null ? deletedRow.Deserialize<TDocument>(_serializerOptions)! : null;

            ProcessSingleRow(insertedDocument, deletedDocument);
        }
    }

    private void ProcessSingleRow(TDocument? insertedDocument, TDocument? deletedDocument) {
        if(insertedDocument != null) {
            if(deletedDocument != null) {
                Updated?.Invoke(insertedDocument, deletedDocument);
            } else {
                Inserted?.Invoke(insertedDocument);
            }
        } else if(deletedDocument != null) {
            Deleted?.Invoke(deletedDocument);
        }
    }

    private void ProcessMultipleRows(Dictionary<int, TDocument> insertedDocuments, Dictionary<int, TDocument> deletedDocuments) {
        foreach(var insertedDocument in insertedDocuments) {
            ProcessSingleRow(insertedDocument.Value, deletedDocuments.TryGetValue(insertedDocument.Key, out var deletedDocument) ? deletedDocument : null);
        }

        foreach(var deletedDocument in deletedDocuments.Where(deletedDocument => !insertedDocuments.TryGetValue(deletedDocument.Key, out _))) {
            ProcessSingleRow(null, deletedDocument.Value);
        }
    }

    private Dictionary<int, TDocument> GetDocuments(JsonNode? affectedRow, bool multipleAffectedRows) {
        var documents = new Dictionary<int, TDocument>();

        if(affectedRow == null)
            return documents;

        if(multipleAffectedRows) {
            foreach(var row in affectedRow.AsArray()) {
                var insertedDocument = row.Deserialize<TDocument>(_serializerOptions);
                var key = DocumentHelpers.CalculateKey(insertedDocument);
                documents[key] = insertedDocument;
            }
        } else {
            var insertedDocument = affectedRow.Deserialize<TDocument>(_serializerOptions);
            var key = DocumentHelpers.CalculateKey(insertedDocument);
            documents[key] = insertedDocument;
        }

        return documents;
    }
}