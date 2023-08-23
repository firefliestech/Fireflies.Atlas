using System.Linq.Expressions;
using Fireflies.Atlas.Core;

namespace Fireflies.Atlas.Source.Redis;

public class RedisKeySource<TDocument> : AtlasSource<TDocument> where TDocument : new() {
    private readonly RedisSource _source;
    private readonly KeyDescriptor _keyDescriptor;

    public RedisKeySource(RedisSource source, KeyDescriptor keyDescriptor) {
        _source = source;
        _keyDescriptor = keyDescriptor;
    }

    public override Task<IEnumerable<(bool Cache, TDocument Document)>> GetDocuments(Expression<Func<TDocument, bool>>? predicate, ExecutionFlags flags) {
        return _source.GetKeyDocuments(predicate, _keyDescriptor);
    }

    public override void Dispose() {
        
    }
}