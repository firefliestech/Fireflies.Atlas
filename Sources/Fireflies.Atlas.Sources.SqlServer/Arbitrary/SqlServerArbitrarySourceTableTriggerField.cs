using System.Linq.Expressions;
using System.Reflection;

namespace Fireflies.Atlas.Sources.SqlServer.Arbitrary;

public class SqlServerArbitrarySourceTableTriggerField {
    public PropertyInfo Property { get; set; }
}

public class SqlServerArbitrarySourceTableTriggerField<TDocument, TProperty> : SqlServerArbitrarySourceTableTriggerField {
    public SqlServerArbitrarySourceTableTriggerField(Expression<Func<TDocument, TProperty>> property) {
        Property = (PropertyInfo)((MemberExpression)property.Body).Member;
    }
}
