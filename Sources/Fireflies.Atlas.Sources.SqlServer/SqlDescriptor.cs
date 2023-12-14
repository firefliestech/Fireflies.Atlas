namespace Fireflies.Atlas.Sources.SqlServer;

public abstract class SqlDescriptor {
    internal abstract string AsSql { get; }

    //public static implicit operator SqlDescriptor(string descriptor) => new(descriptor);
}

public class SqlNameDescriptor : SqlDescriptor {
    private string _schema = null!;
    private string _table = null!;

    internal override string AsSql => $"{Schema}.{Table}";

    public string Schema {
        get => _schema;
        set {
            if(value[0] != '[')
                value = $"[{value}]";

            _schema = value;
        }
    }

    public string Table {
        get => _table;
        set {
            if(value[0] != '[')
                value = $"[{value}]";

            _table = value;
        }
    }

    public SqlNameDescriptor(string schema, string table) {
        Schema = schema;
        Table = table;
    }

    public SqlNameDescriptor(string descriptor) {
        var parts = descriptor.Split(".");
        switch(parts.Length) {
            case 1:
                Schema = "dbo";
                Table = parts[0];
                break;
            case 2:
                Schema = parts[0];
                Table = parts[1];
                break;
            default:
                throw new ArgumentException($"{nameof(descriptor)} needs to be name or schema.name");
        }
    }

    protected bool Equals(SqlNameDescriptor other) {
        return _schema.ToUpper() == other._schema.ToUpper() && _table.ToUpper() == other._table.ToUpper();
    }

    public override bool Equals(object? obj) {
        if(ReferenceEquals(null, obj)) return false;
        if(ReferenceEquals(this, obj)) return true;
        if(obj.GetType() != this.GetType()) return false;

        return Equals((SqlNameDescriptor)obj);
    }

    public override int GetHashCode() {
        return HashCode.Combine(_schema.ToUpper(), _table.ToUpper());
    }

    public override string ToString() {
        return $"{Schema}.{Table}";
    }
}

public class SqlQueryDescriptor : SqlDescriptor {
    private readonly string _query;

    internal override string AsSql => $"({_query}) AQ";

    public SqlQueryDescriptor(string query) {
        _query = query;
    }

    protected bool Equals(SqlQueryDescriptor other) {
        return _query == other._query;
    }

    public override bool Equals(object? obj) {
        if(ReferenceEquals(null, obj)) return false;
        if(ReferenceEquals(this, obj)) return true;
        if(obj.GetType() != this.GetType()) return false;

        return Equals((SqlQueryDescriptor)obj);
    }

    public override int GetHashCode() {
        return _query.GetHashCode();
    }
}