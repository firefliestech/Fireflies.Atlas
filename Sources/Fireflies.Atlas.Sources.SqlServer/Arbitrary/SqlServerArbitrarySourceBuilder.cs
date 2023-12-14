using System.Linq.Expressions;

namespace Fireflies.Atlas.Sources.SqlServer.Arbitrary;

public class SqlServerArbitrarySourceBuilder<TDocument> {
    private readonly List<SqlServerArbitrarySourceTableTriggerBuilder> _tableTriggerBuilder = new();

    internal IEnumerable<SqlServerArbitrarySourceTableTriggerBuilder> TableTriggerBuilders => _tableTriggerBuilder;
    internal Expression<Func<TDocument, bool>>? Filter { get; private set; }
    internal bool CacheEnabled { get; private set; } = true;

    public SqlServerArbitrarySourceTableTriggerBuilder<TDocument> AddTableTrigger(SqlNameDescriptor sqlDescriptor) {
        var builder = new SqlServerArbitrarySourceTableTriggerBuilder<TDocument>(sqlDescriptor);
        _tableTriggerBuilder.Add(builder);
        return builder;
    }

    public SqlServerArbitrarySourceBuilder<TDocument> DisableCache() {
        CacheEnabled = false;
        return this;
    }

    public SqlServerArbitrarySourceBuilder<TDocument> SetFilter(Expression<Func<TDocument, bool>> filter) {
        Filter = filter;
        return this;
    }
}