namespace Fireflies.Atlas.Core.Linq;

public abstract class AtlasContext {
    private readonly Atlas _atlas;

    protected AtlasContext(Atlas atlas) {
        _atlas = atlas;
    }

    protected IQueryable<TDocument> CreateQueryable<TDocument>() where TDocument : new() {
        return new Queryable<TDocument>(new AtlasQueryProvider<TDocument>(_atlas));
    }
}