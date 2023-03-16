using System.Linq.Expressions;

namespace Fireflies.Atlas.Core;

public abstract class AtlasSource : IDisposable {
    public abstract Task<(bool Cache, IEnumerable<TDocument> Documents)> GetDocuments<TDocument>(Expression<Func<TDocument, bool>>? predicate) where TDocument : new();
    public abstract void Dispose();
}