using System.Linq.Expressions;
using System.Reflection;

namespace Fireflies.Atlas.Core.Linq;

internal class QueryProvider : IQueryProvider {
    private readonly IQueryContext queryContext;

    public QueryProvider(IQueryContext queryContext) {
        this.queryContext = queryContext;
    }

    public virtual IQueryable CreateQuery(Expression expression) {
        var elementType = TypeSystem.GetElementType(expression.Type);
        try {
            return (IQueryable)Activator.CreateInstance(typeof(Queryable<>).MakeGenericType(elementType), new object[] { this, expression });
        } catch (TargetInvocationException e) {
            throw e.InnerException;
        }
    }

    public virtual IQueryable<T> CreateQuery<T>(Expression expression) {
        return new Queryable<T>(this, expression);
    }

    object IQueryProvider.Execute(Expression expression) {
        return queryContext.Execute(expression, false);
    }

    T IQueryProvider.Execute<T>(Expression expression) {
        var result = queryContext.Execute(expression, (typeof(T).Name == "IEnumerable`1"));
        return (T)result;
    }
}