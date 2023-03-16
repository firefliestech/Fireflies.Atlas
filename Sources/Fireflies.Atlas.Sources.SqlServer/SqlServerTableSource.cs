using System.Linq.Expressions;
using Fireflies.Atlas.Core;

namespace Fireflies.Atlas.Sources.SqlServer;

public class SqlServerTableSource : AtlasSource {
    private readonly SqlServerSource _source;
    private readonly TableDescriptor _tableDescriptor;

    public SqlServerTableSource(SqlServerSource source, TableDescriptor tableDescriptor) {
        _source = source;
        _tableDescriptor = tableDescriptor;
    }

    public override Task<(bool Cache, IEnumerable<TDocument> Documents)> GetDocuments<TDocument>(Expression<Func<TDocument, bool>>? predicate) {
        return _source.GetDocuments(predicate, _tableDescriptor);
    }

    public override void Dispose() {
        _source.Dispose();
    }
}