using System.Linq.Expressions;
using Fireflies.Atlas.Core;

namespace Fireflies.Atlas.Source.Redis;

public class RedisHashSource<TDocument> : AtlasSource<TDocument> where TDocument : new() {
    private readonly RedisSource _source;
    private readonly HashDescriptor _hashDescriptor;

    public RedisHashSource(RedisSource source, HashDescriptor hashDescriptor) {
        _source = source;
        _hashDescriptor = hashDescriptor;
    }

    public override Task<(bool Cache, IEnumerable<TDocument> Documents)> GetDocuments(Expression<Func<TDocument, bool>>? predicate) {
        return _source.GetDocuments(predicate, _hashDescriptor);
    }

    public override void Dispose() {
        
    }
}