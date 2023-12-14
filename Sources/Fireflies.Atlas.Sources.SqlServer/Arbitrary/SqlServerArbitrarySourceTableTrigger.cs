using System.Reflection;
using Fireflies.Atlas.Annotations;

namespace Fireflies.Atlas.Sources.SqlServer.Arbitrary;

public class SqlServerArbitrarySourceTableTrigger {
    private readonly List<SqlServerArbitrarySourceTableTriggerField> _fields;
    private readonly bool _hasKeyField;

    public SqlNameDescriptor SqlDescriptor { get; }
    public Type Type { get; set; }

    public IEnumerable<SqlServerArbitrarySourceTableTriggerField> Fields => _fields;
    public bool HasKeyField => _hasKeyField;

    public SqlServerArbitrarySourceTableTrigger(SqlNameDescriptor sqlDescriptor, List<SqlServerArbitrarySourceTableTriggerField> fields) {
        SqlDescriptor = sqlDescriptor;
        _fields = fields;
        _hasKeyField = fields.Any(x => x.Property.GetCustomAttribute<AtlasKeyAttribute>() != null);
    }
}

public class SqlServerArbitrarySourceTableTrigger<TDocument> : SqlServerArbitrarySourceTableTrigger {
    public SqlServerArbitrarySourceTableTrigger(SqlNameDescriptor sqlDescriptor, List<SqlServerArbitrarySourceTableTriggerField> fields) : base(sqlDescriptor, fields) {
        Type = typeof(TDocument);
    }
}