using System.Linq.Expressions;

namespace Fireflies.Atlas.Core.Linq;

internal class AtlasQueryProvider<TDocument> : IQueryContext where TDocument : new() {
    private readonly Atlas _atlas;

    public AtlasQueryProvider(Atlas atlas) {
        _atlas = atlas;
    }

    public object Execute(Expression expression, bool isEnumerable) {
        if(isEnumerable)
            return _atlas.GetDocuments<TDocument>(expression).Result;

        return _atlas.GetDocument<TDocument>(expression).Result;
    }
}