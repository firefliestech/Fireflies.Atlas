using System.Collections.Concurrent;
using System.Linq.Expressions;
using FastExpressionCompiler;

namespace Fireflies.Atlas.Core;

public static class ExpressionCompiler {
    private static readonly ConcurrentDictionary<int, ConcurrentDictionary<int, System.Delegate>> Cache = new();

    public static Func<TDocument, TReturn> Compile<TDocument, TReturn>(Expression<Func<TDocument, TReturn>> exp) {
        var typeHash = new HashCode();
        typeHash.Add(typeof(TDocument));
        typeHash.Add(typeof(TReturn));

        var typeCache = Cache.GetOrAdd(typeHash.ToHashCode(), new ConcurrentDictionary<int, System.Delegate>());
        var expressionHash = ExpressionHasher.GetHashCode(exp);

        if(typeCache.TryGetValue(expressionHash, out var cachedLambda))
            return (Func<TDocument, TReturn>)cachedLambda;

        var newLambda = InternalCompile(exp);
        typeCache[expressionHash] = newLambda;
        return newLambda;
    }

    private static Func<TDocument, TReturn> InternalCompile<TDocument, TReturn>(Expression<Func<TDocument, TReturn>> exp) {
        return exp.CompileFast();
    }
}