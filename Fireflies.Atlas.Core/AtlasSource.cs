using System.Linq.Expressions;

namespace Fireflies.Atlas.Core;

public abstract class AtlasSource : IDisposable {
    public abstract void Dispose();

    public virtual void Validate(AtlasDocumentBuilder atlasBuilder) {
    }
}

public abstract class AtlasSource<TDocument> : AtlasSource where TDocument : new() {
    public abstract Task<IEnumerable<(bool Cache, TDocument Document)>> GetDocuments(Expression<Func<TDocument, bool>>? predicate, ExecutionFlags flags);
}