namespace Fireflies.Atlas.Core;

public interface IDocumentDictionary<out TDocument> {
    event Action<TDocument> Loaded;
    event Action<TDocument> Updated;
    event Action<TDocument> Deleted;
}