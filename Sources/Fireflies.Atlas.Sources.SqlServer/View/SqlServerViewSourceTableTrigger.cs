namespace Fireflies.Atlas.Sources.SqlServer.View;

public class SqlServerViewSourceTableTrigger<TDocument> : SqlServerViewSourceTableTrigger {
    public SqlServerViewSourceTableTrigger(SqlDescriptor sqlDescriptor, List<SqlServerViewSourceTableTriggerField> fields) : base(sqlDescriptor, fields) {
        Type = typeof(TDocument);
    }
}

public class SqlServerViewSourceTableTrigger {
    private readonly List<SqlServerViewSourceTableTriggerField> _fields;

    public SqlDescriptor SqlDescriptor { get; }
    public Type Type { get; set; }

    public IEnumerable<SqlServerViewSourceTableTriggerField> Fields => _fields;

    public SqlServerViewSourceTableTrigger(SqlDescriptor sqlDescriptor, List<SqlServerViewSourceTableTriggerField> fields) {
        SqlDescriptor = sqlDescriptor;
        _fields = fields;
    }
}