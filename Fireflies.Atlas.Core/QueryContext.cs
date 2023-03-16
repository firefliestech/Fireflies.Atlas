using System.Collections.Concurrent;
using System.Linq.Expressions;

namespace Fireflies.Atlas.Core;

public class QueryContext {
    public ConcurrentDictionary<Type, ConcurrentDictionary<string, object>> _cache = new();

    public IEnumerable<TDocument> Add<TDocument>(Expression<Func<TDocument, bool>>? expression, TDocument[] documents) where TDocument : new() {
        var documentDictionary = _cache.GetOrAdd(typeof(TDocument), _ => new ConcurrentDictionary<string, object>());
        documentDictionary.AddOrUpdate(expression?.ToString() ?? "<NULL>", _ => documents, (_, _) => documents);
        return documents;
    }

    public bool Contains<TDocument>(Expression<Func<TDocument, bool>>? expression) where TDocument : new() {
        return _cache.TryGetValue(typeof(TDocument), out var existingDictionary) && existingDictionary.TryGetValue(expression?.ToString() ?? "<NULL>", out _);
    }

    public IEnumerable<TDocument> Get<TDocument>(Expression<Func<TDocument, bool>>? expression) where TDocument : new() {
        if(_cache.TryGetValue(typeof(TDocument), out var existingDictionary) && existingDictionary.TryGetValue(expression?.ToString() ?? "<NULL>", out var documents)) {
            return (IEnumerable<TDocument>)documents;
        }

        return Enumerable.Empty<TDocument>();
    }
}