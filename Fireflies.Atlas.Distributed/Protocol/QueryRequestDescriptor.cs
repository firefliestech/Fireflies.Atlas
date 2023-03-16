using System.Linq.Expressions;

namespace Fireflies.Atlas.Distributed.Protocol;

public class QueryRequestDescriptor {
    public Guid Uuid { get; set; }
    public bool EnableNotifications { get; set; }
    public LambdaExpression Expression { get; set; }
    public Type DocumentType => Expression.Parameters.First().Type;

    internal virtual void SetResult(IEnumerable<object> result) {
    }
}