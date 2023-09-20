using Fireflies.Atlas.Core.Delegate;

namespace Fireflies.Atlas.Core;

public interface IDocumentDictionary<out TDocument> {
    event DocumentLoaded<TDocument>? Loaded;
    event DocumentUpdated<TDocument>? Updated;
    event DocumentDeleted<TDocument>? Deleted;
}