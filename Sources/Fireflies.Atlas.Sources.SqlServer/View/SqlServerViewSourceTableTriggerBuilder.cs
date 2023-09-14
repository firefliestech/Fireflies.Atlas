using System.Linq.Expressions;

namespace Fireflies.Atlas.Sources.SqlServer.View;

public abstract class SqlServerViewSourceTableTriggerBuilder {
    public abstract SqlServerViewSourceTableTrigger Build();
}

public class SqlServerViewSourceTableTriggerBuilder<TDocument> : SqlServerViewSourceTableTriggerBuilder {
    private readonly SqlDescriptor _sqlDescriptor;
    private readonly List<SqlServerViewSourceTableTriggerField> _fields = new();

    public SqlServerViewSourceTableTriggerBuilder(SqlDescriptor sqlDescriptor) {
        _sqlDescriptor = sqlDescriptor;
    }

    public SqlServerViewSourceTableTriggerBuilder<TDocument> AddField<TProperty>(Expression<Func<TDocument, TProperty>> property) {
        _fields.Add(new SqlServerViewSourceTableTriggerField<TDocument, TProperty>(property));
        return this;
    }

    public override SqlServerViewSourceTableTrigger Build() {
        return new SqlServerViewSourceTableTrigger<TDocument>(_sqlDescriptor, _fields);
    }
}
