using System.Collections.Concurrent;
using System.Linq.Expressions;
using FastExpressionCompiler;

namespace Fireflies.Atlas.Core;

public static class ExpressionCompiler {
    private static readonly ConcurrentDictionary<int, Delegate> Cache = new();

    public static Func<TDocument, TReturn> Compile<TDocument, TReturn>(Expression<Func<TDocument, TReturn>> exp) {
        var key = ExpressionHasher.GetHashCode(exp);
        return (Func<TDocument, TReturn>)Cache.GetOrAdd(key, exp.CompileFast());
    }
}