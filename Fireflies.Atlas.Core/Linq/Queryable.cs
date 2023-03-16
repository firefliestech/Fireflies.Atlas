using System.Linq.Expressions;

namespace Fireflies.Atlas.Core.Linq;

internal class Queryable<T> : IOrderedQueryable<T> {
    public Queryable(IQueryContext queryContext) {
        Initialize(new QueryProvider(queryContext), null);
    }

    internal Queryable(IQueryProvider provider, Expression expression) {
        Initialize(provider, expression);
    }

    private void Initialize(IQueryProvider provider, Expression expression) {
        if (expression != null && !typeof(IQueryable<T>).IsAssignableFrom(expression.Type))
            throw new ArgumentException($"Not assignable from {expression.Type}", nameof(expression));

        Provider = provider ?? throw new ArgumentNullException(nameof(provider));
        Expression = expression ?? Expression.Constant(this);
    }

    public IEnumerator<T> GetEnumerator() {
        return (Provider.Execute<IEnumerable<T>>(Expression)).GetEnumerator();
    }

    System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator() {
        return (Provider.Execute<System.Collections.IEnumerable>(Expression)).GetEnumerator();
    }

    public Type ElementType => typeof(T);

    public Expression Expression { get; private set; }
    public IQueryProvider Provider { get; private set; }
}