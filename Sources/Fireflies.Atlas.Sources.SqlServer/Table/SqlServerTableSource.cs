using System.Linq.Expressions;
using Fireflies.Atlas.Core;

namespace Fireflies.Atlas.Sources.SqlServer.Table;

public class SqlServerTableSource<TDocument> : AtlasSource<TDocument> where TDocument : class, new() {
    private readonly Core.Atlas _atlas;
    private readonly SqlServerSource _source;
    private readonly SqlNameDescriptor _tableDescriptor;
    private readonly Expression<Func<TDocument, bool>>? _filterExpression;
    private readonly Func<TDocument, bool>? _compiledFilter;
    private readonly bool _cacheEnabled;

    public SqlServerTableSource(Core.Atlas atlas, SqlServerSource source, SqlNameDescriptor tableDescriptor, SqlServerTableSourceBuilder<TDocument> builder) {
        _atlas = atlas;
        _source = source;
        _tableDescriptor = tableDescriptor;
        _filterExpression = builder.Filter;
        _compiledFilter = _filterExpression?.Compile();
        _cacheEnabled = builder.CacheEnabled;

        if(builder.TableMonitorEnabled) {
            var monitor = _source.UpdateFetcher.MonitorTable<TDocument>(_tableDescriptor);
            monitor.Deleted += MonitorOnDeleted;
            monitor.Inserted += MonitorOnInserted;
            monitor.Updated += MonitorOnUpdated;
        }
    }

    public override void Validate(AtlasDocumentBuilder atlasBuilder) {
        if(atlasBuilder.Preload && !_cacheEnabled)
            throw new AtlasException($"If {nameof(atlasBuilder.Preload)} is enabled cache cant´t be disabled");
    }

    public override async Task<IEnumerable<(bool Cache, TDocument Document)>> GetDocuments(Expression<Func<TDocument, bool>>? predicate, ExecutionFlags flags) {
        var result = await _source.GetDocuments(predicate, _tableDescriptor, _filterExpression, flags).ConfigureAwait(false);
        if(_compiledFilter == null || !_cacheEnabled)
            return result.Select(x => (_cacheEnabled, x));

        return result.Select(x => (_compiledFilter(x), x));
    }

    private void MonitorOnUpdated(TDocument newDocument, TDocument deletedDocument) {
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