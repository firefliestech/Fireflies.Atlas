namespace Fireflies.Atlas.Sources.SqlServer;

public class SqlDescriptor {
    private string _schema = null!;
    private string _table = null!;

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

    public SqlDescriptor(string schema, string table) {
        Schema = schema;
        Table = table;
    }

    public SqlDescriptor(string descriptor) {
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

    protected bool Equals(SqlDescriptor other) {
        return _schema.ToUpper() == other._schema.ToUpper() && _table.ToUpper() == other._table.ToUpper();
    }

    public override bool Equals(object? obj) {
        if(ReferenceEquals(null, obj)) return false;
        if(ReferenceEquals(this, obj)) return true;
        if(obj.GetType() != this.GetType()) return false;

        return Equals((SqlDescriptor)obj);
    }

    public override int GetHashCode() {
        return HashCode.Combine(_schema.ToUpper(), _table.ToUpper());
    }

    public override string ToString() {
        return $"{Schema}.{Table}";
    }

    public static implicit operator SqlDescriptor(string descriptor) => new(descriptor);
}