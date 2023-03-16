namespace Fireflies.Atlas.Sources.SqlServer;

public class TableDescriptor {
    private string _schema;
    private string _table;

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

    protected bool Equals(TableDescriptor other) {
        return _schema.ToUpper() == other._schema.ToUpper() && _table.ToUpper() == other._table.ToUpper();
    }

    public override bool Equals(object? obj) {
        if(ReferenceEquals(null, obj)) return false;
        if(ReferenceEquals(this, obj)) return true;
        if(obj.GetType() != this.GetType()) return false;

        return Equals((TableDescriptor)obj);
    }

    public override int GetHashCode() {
        return HashCode.Combine(_schema.ToUpper(), _table.ToUpper());
    }

    public override string ToString() {
        return $"{Schema}.{Table}";
    }
}