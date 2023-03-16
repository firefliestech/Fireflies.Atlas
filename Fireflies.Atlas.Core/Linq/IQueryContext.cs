using System.Linq.Expressions;

namespace Fireflies.Atlas.Core.Linq;

public interface IQueryContext {
    object Execute(Expression expression, bool isEnumerable);
}