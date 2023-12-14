using System.Linq.Expressions;

namespace Fireflies.Atlas.Sources.SqlServer.Arbitrary;

public abstract class SqlServerArbitrarySourceTableTriggerBuilder {
    public abstract SqlServerArbitrarySourceTableTrigger Build();
}

public class SqlServerArbitrarySourceTableTriggerBuilder<TDocument> : SqlServerArbitrarySourceTableTriggerBuilder {
    private readonly SqlNameDescriptor _sqlDescriptor;
    private readonly List<SqlServerArbitrarySourceTableTriggerField> _fields = new();

    public SqlServerArbitrarySourceTableTriggerBuilder(SqlNameDescriptor sqlDescriptor) {
        _sqlDescriptor = sqlDescriptor;
    }

    public SqlServerArbitrarySourceTableTriggerBuilder<TDocument> AddField<TProperty>(Expression<Func<TDocument, TProperty>> property) {
        _fields.Add(new SqlServerArbitrarySourceTableTriggerField<TDocument, TProperty>(property));
        return this;
    }

    public override SqlServerArbitrarySourceTableTrigger Build() {
        return new SqlServerArbitrarySourceTableTrigger<TDocument>(_sqlDescriptor, _fields);
    }
}
