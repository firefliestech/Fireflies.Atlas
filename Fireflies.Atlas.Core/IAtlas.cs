using System.Linq.Expressions;

namespace Fireflies.Atlas.Core;

public interface IAtlas : IDisposable {
    Task<TDocument?> GetDocument<TDocument>(Expression<Func<TDocument, bool>> predicate, CacheFlag cacheFlag = CacheFlag.Default, ExecutionFlags flags = ExecutionFlags.None) where TDocument : new();
    Task<TDocument?> GetDocument<TDocument>(Expression predicate, CacheFlag cacheFlag = CacheFlag.Default, ExecutionFlags flags = ExecutionFlags.None) where TDocument : new();
    Task<IEnumerable<TDocument>> GetDocuments<TDocument>(CacheFlag cacheFlag = CacheFlag.Default, ExecutionFlags flags = ExecutionFlags.None) where TDocument : new();
    Task<IEnumerable<TDocument>> GetDocuments<TDocument>(Expression<Func<TDocument, bool>> predicate, CacheFlag cacheFlag = CacheFlag.Default, ExecutionFlags flags = ExecutionFlags.None) where TDocument : new();
    Task<IEnumerable<TDocument>> GetDocuments<TDocument>(Expression predicate, CacheFlag cacheFlag = CacheFlag.Default, ExecutionFlags flags = ExecutionFlags.None) where TDocument : new();
    IDocumentDictionary<TDocument>? GetDictionary<TDocument>() where TDocument : new();
    Task TriggerUpdate<TDocument>(Expression<Func<TDocument, bool>> predicate) where TDocument : new();
}