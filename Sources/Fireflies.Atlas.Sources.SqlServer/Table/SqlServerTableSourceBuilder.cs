using System.Linq.Expressions;

namespace Fireflies.Atlas.Sources.SqlServer.Table;

public class SqlServerTableSourceBuilder<TDocument> {
    internal bool TableMonitorEnabled { get; private set; } = true;
    internal Expression<Func<TDocument, bool>>? Filter { get; private set; }
    internal bool CacheEnabled { get; private set; } = true;

    public SqlServerTableSourceBuilder<TDocument> DisableTableMonitor() {
        TableMonitorEnabled = false;
        return this;
    }

    public SqlServerTableSourceBuilder<TDocument> DisableCache() {
        CacheEnabled = false;
        return this;
    }

    public SqlServerTableSourceBuilder<TDocument> SetFilter(Expression<Func<TDocument, bool>> filter) {
        Filter = filter;
        return this;
    }
}