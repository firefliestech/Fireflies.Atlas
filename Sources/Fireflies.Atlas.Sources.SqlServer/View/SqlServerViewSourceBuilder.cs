namespace Fireflies.Atlas.Sources.SqlServer.View;

public class SqlServerViewSourceBuilder<TDocument> {
    private readonly List<SqlServerViewSourceTableTriggerBuilder> _tableTriggerBuilder = new();

    public IEnumerable<SqlServerViewSourceTableTriggerBuilder> TableTriggerBuilders => _tableTriggerBuilder;

    public SqlServerViewSourceTableTriggerBuilder<TDocument> AddTableTrigger(SqlDescriptor sqlDescriptor) {
        var builder = new SqlServerViewSourceTableTriggerBuilder<TDocument>(sqlDescriptor);
        _tableTriggerBuilder.Add(builder);
        return builder;
    }
}