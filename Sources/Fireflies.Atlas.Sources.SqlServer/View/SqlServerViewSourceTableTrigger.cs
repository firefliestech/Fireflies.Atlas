using System.Reflection;
using Fireflies.Atlas.Annotations;

namespace Fireflies.Atlas.Sources.SqlServer.View;

public class SqlServerViewSourceTableTrigger {
    private readonly List<SqlServerViewSourceTableTriggerField> _fields;
    private readonly bool _hasKeyField;

    public SqlDescriptor SqlDescriptor { get; }
    public Type Type { get; set; }

    public IEnumerable<SqlServerViewSourceTableTriggerField> Fields => _fields;
    public bool HasKeyField => _hasKeyField;

    public SqlServerViewSourceTableTrigger(SqlDescriptor sqlDescriptor, List<SqlServerViewSourceTableTriggerField> fields) {
        SqlDescriptor = sqlDescriptor;
        _fields = fields;
        _hasKeyField = fields.Any(x => x.Property.GetCustomAttribute<AtlasKeyAttribute>() != null);
    }
}

public class SqlServerViewSourceTableTrigger<TDocument> : SqlServerViewSourceTableTrigger {
    public SqlServerViewSourceTableTrigger(SqlDescriptor sqlDescriptor, List<SqlServerViewSourceTableTriggerField> fields) : base(sqlDescriptor, fields) {
        Type = typeof(TDocument);
    }
}