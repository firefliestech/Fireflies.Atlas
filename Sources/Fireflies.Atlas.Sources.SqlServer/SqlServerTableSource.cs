using System.Linq.Expressions;
using Fireflies.Atlas.Core;

namespace Fireflies.Atlas.Sources.SqlServer;

public class SqlServerTableSource<TDocument> : AtlasSource<TDocument> where TDocument : new() {
    private readonly SqlServerSource _source;
    private readonly TableDescriptor _tableDescriptor;
    private readonly Expression<Func<TDocument, bool>>? _filter;

    public SqlServerTableSource(SqlServerSource source, TableDescriptor tableDescriptor, Expression<Func<TDocument, bool>>? filter) {
        _source = source;
        _tableDescriptor = tableDescriptor;
        _filter = filter;
    }

    public override Task<(bool Cache, IEnumerable<TDocument> Documents)> GetDocuments(Expression<Func<TDocument, bool>>? predicate) {
        return _source.GetDocuments(predicate, _tableDescriptor, _filter);
    }

    public override void Dispose() {
        _source.Dispose();
    }
}