using System.Linq.Expressions;

namespace Fireflies.Atlas.Core;

public abstract class AtlasSource : IDisposable {
    public abstract void Dispose();
}

public abstract class AtlasSource<TDocument> : AtlasSource where TDocument : new() {
    public abstract Task<(bool Cache, IEnumerable<TDocument> Documents)> GetDocuments(Expression<Func<TDocument, bool>>? predicate, ExecutionFlags flags);
}