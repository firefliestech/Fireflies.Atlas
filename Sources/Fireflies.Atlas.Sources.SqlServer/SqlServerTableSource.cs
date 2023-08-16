using System.Linq.Expressions;
using Fireflies.Atlas.Core;

namespace Fireflies.Atlas.Sources.SqlServer;

public class SqlServerTableSource<TDocument> : AtlasSource<TDocument> where TDocument : new() {
    private readonly SqlServerSource _source;
    private readonly TableDescriptor _tableDescriptor;
    private readonly Expression<Func<TDocument, bool>>? _filterExpression;
    private readonly Func<TDocument, bool>? _compiledFilter;

    public SqlServerTableSource(SqlServerSource source, TableDescriptor tableDescriptor, Expression<Func<TDocument, bool>>? filterExpression) {
        _source = source;
        _tableDescriptor = tableDescriptor;
        _filterExpression = filterExpression;
        _compiledFilter = filterExpression?.Compile();
    }

    public override async Task<IEnumerable<(bool Cache, TDocument Document)>> GetDocuments(Expression<Func<TDocument, bool>>? predicate, ExecutionFlags flags) {
        var result = await _source.GetDocuments(predicate, _tableDescriptor, _filterExpression, flags).ConfigureAwait(false);
        if(_compiledFilter == null)
            return result.Select(x => (true, x));

        return result.Select(x => (_compiledFilter(x), x));
    }

    public override void Dispose() {
        _source.Dispose();
    }
}
