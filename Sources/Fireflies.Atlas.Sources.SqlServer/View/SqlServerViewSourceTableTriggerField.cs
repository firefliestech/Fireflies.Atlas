using System.Linq.Expressions;
using System.Reflection;

namespace Fireflies.Atlas.Sources.SqlServer.View;

public class SqlServerViewSourceTableTriggerField {
    public PropertyInfo Property { get; set; }
}

public class SqlServerViewSourceTableTriggerField<TDocument, TProperty> : SqlServerViewSourceTableTriggerField {
    public SqlServerViewSourceTableTriggerField(Expression<Func<TDocument, TProperty>> property) {
        Property = (PropertyInfo)((MemberExpression)property.Body).Member;
    }
}
