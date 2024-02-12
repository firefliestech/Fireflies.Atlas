using System.Linq.Expressions;

namespace Fireflies.Atlas.Sources.SqlServer;

public interface IMethodCallSqlExtender {
    (bool, string?) Handle(MethodCallExpression node);
}