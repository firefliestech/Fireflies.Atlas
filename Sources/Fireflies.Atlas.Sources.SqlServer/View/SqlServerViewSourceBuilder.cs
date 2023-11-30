using System.Linq.Expressions;

namespace Fireflies.Atlas.Sources.SqlServer.View;

public class SqlServerViewSourceBuilder<TDocument> {
    private readonly List<SqlServerViewSourceTableTriggerBuilder> _tableTriggerBuilder = new();

    internal IEnumerable<SqlServerViewSourceTableTriggerBuilder> TableTriggerBuilders => _tableTriggerBuilder;
    internal Expression<Func<TDocument, bool>>? Filter { get; private set; }
    internal bool CacheEnabled { get; private set; } = true;

    public SqlServerViewSourceTableTriggerBuilder<TDocument> AddTableTrigger(SqlDescriptor sqlDescriptor) {
        var builder = new SqlServerViewSourceTableTriggerBuilder<TDocument>(sqlDescriptor);
        _tableTriggerBuilder.Add(builder);
        return builder;
    }

    public SqlServerViewSourceBuilder<TDocument> DisableCache() {
        CacheEnabled = false;
        return this;
    }

    public SqlServerViewSourceBuilder<TDocument> SetFilter(Expression<Func<TDocument, bool>> filter) {
        Filter = filter;
        return this;
    }
}