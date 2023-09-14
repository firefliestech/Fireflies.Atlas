using System.Linq.Expressions;
using Fireflies.Atlas.Core;

namespace Fireflies.Atlas.Sources.SqlServer.Table;

public class SqlServerTableSource<TDocument> : AtlasSource<TDocument> where TDocument : new() {
    private readonly Core.Atlas _atlas;
    private readonly SqlServerSource _source;
    private readonly SqlDescriptor _tableDescriptor;
    private readonly Expression<Func<TDocument, bool>>? _filterExpression;
    private readonly Func<TDocument, bool>? _compiledFilter;

    public SqlServerTableSource(Core.Atlas atlas, SqlServerSource source, SqlDescriptor tableDescriptor, Expression<Func<TDocument, bool>>? filterExpression) {
        _atlas = atlas;
        _source = source;
        _tableDescriptor = tableDescriptor;
        _filterExpression = filterExpression;
        _compiledFilter = filterExpression?.Compile();

        var monitor = _source.UpdateFetcher.MonitorTable<TDocument>(_tableDescriptor);
        monitor.Deleted += MonitorOnDeleted;
        monitor.Inserted += MonitorOnInserted;
        monitor.Updated += MonitorOnUpdated;
    }

    public override async Task<IEnumerable<(bool Cache, TDocument Document)>> GetDocuments(Expression<Func<TDocument, bool>>? predicate, ExecutionFlags flags) {
        var result = await _source.GetDocuments(predicate, _tableDescriptor, _filterExpression, flags).ConfigureAwait(false);
        if(_compiledFilter == null)
            return result.Select(x => (true, x));

        return result.Select(x => (_compiledFilter(x), x));
    }

    private void MonitorOnUpdated(TDocument newDocument, Lazy<TDocument> deletedDocument) {
        HandleUpsert(newDocument);
    }

    private void MonitorOnInserted(TDocument newDocument) {
        HandleUpsert(newDocument);
    }

    private void MonitorOnDeleted(TDocument deletedDocument) {
        _atlas.DeleteDocument(deletedDocument);
    }

    private void HandleUpsert(TDocument newDocument) {
        if(_compiledFilter != null && !_compiledFilter(newDocument)) {
            _atlas.DeleteDocument(newDocument);
        } else {
            _atlas.UpdateDocument(newDocument);
        }
    }

    public override void Dispose() {
        _source.Dispose();
    }
}