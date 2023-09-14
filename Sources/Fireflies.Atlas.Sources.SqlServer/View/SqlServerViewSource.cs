using System.Linq.Expressions;
using Fireflies.Atlas.Core;
using Fireflies.Atlas.Core.Helpers;

namespace Fireflies.Atlas.Sources.SqlServer.View;

public class SqlServerViewSource<TDocument> : AtlasSource<TDocument> where TDocument : new() {
    private readonly Core.Atlas _atlas;
    private readonly SqlServerSource _source;
    private readonly SqlDescriptor _viewDescriptor;
    private readonly Expression<Func<TDocument, bool>>? _filterExpression;
    private readonly Func<TDocument, bool>? _compiledFilter;

    public SqlServerViewSource(Core.Atlas atlas, SqlServerSource source, SqlDescriptor viewDescriptor, IEnumerable<SqlServerViewSourceTableTrigger> tableTriggers, Expression<Func<TDocument, bool>>? filterExpression) {
        _atlas = atlas;
        _source = source;
        _viewDescriptor = viewDescriptor;
        _filterExpression = filterExpression;
        _compiledFilter = filterExpression?.Compile();

        foreach(var trigger in tableTriggers) {
            var monitor = _source.UpdateFetcher.MonitorTable<TDocument>(trigger.SqlDescriptor);

            monitor.Deleted += deletedDocument => MonitorOnDeleted(trigger, deletedDocument);
            monitor.Inserted += insertedDocument => MonitorOnInserted(trigger, insertedDocument);
            monitor.Updated += (updatedDocument, deletedDocument) => MonitorOnUpdated(trigger, updatedDocument, deletedDocument);
        }
    }

    public override async Task<IEnumerable<(bool Cache, TDocument Document)>> GetDocuments(Expression<Func<TDocument, bool>>? predicate, ExecutionFlags flags) {
        var result = await _source.GetDocuments(predicate, _viewDescriptor, _filterExpression, flags).ConfigureAwait(false);
        if(_compiledFilter == null)
            return result.Select(x => (true, x));

        return result.Select(x => (_compiledFilter(x), x));
    }

    private void MonitorOnUpdated(SqlServerViewSourceTableTrigger trigger, TDocument newDocument, Lazy<TDocument> deletedDocument) {
        var queryExpression = BuildTriggerExpressionFromDocument(trigger, deletedDocument.Value);
        HandleUpsert(queryExpression);
    }

    private void MonitorOnInserted(SqlServerViewSourceTableTrigger trigger, TDocument newDocument) {
        var queryExpression = BuildTriggerExpressionFromDocument(trigger, newDocument);
        HandleUpsert(queryExpression);
    }

    private async void MonitorOnDeleted(SqlServerViewSourceTableTrigger trigger, TDocument deletedDocument) {
        var queryExpression = BuildTriggerExpressionFromDocument(trigger, deletedDocument);
        var currentDocument = await _atlas.GetDocument(queryExpression, CacheFlag.OnlyCache).ConfigureAwait(false);
        if(currentDocument != null)
            _atlas.DeleteDocument(deletedDocument);
    }

    private async void HandleUpsert(Expression<Func<TDocument, bool>> triggerExpression) {
        var documentsThatNeedsToBeReloaded = await _atlas.GetDocuments(triggerExpression, CacheFlag.OnlyCache).ConfigureAwait(false);
        foreach(var currentDocument in documentsThatNeedsToBeReloaded) {
            var keyQuery = BuildViewExpressionFromDocument(currentDocument);
            var viewDocuments = await GetDocuments(keyQuery, ExecutionFlags.None).ConfigureAwait(false);
            var viewDocument = viewDocuments.First().Document;
            if(viewDocument == null) {
                // Document not returned from view
                if(currentDocument != null) {
                    // Did we have in in cache?
                    _atlas.DeleteDocument(currentDocument); // Then delete it
                }
            } else if(_compiledFilter != null && !_compiledFilter(viewDocument)) {
                // We have a new document from view, does it match the filter?
                _atlas.DeleteDocument(viewDocument); // Otherwise delete it
            } else {
                // Otherwise, lets update the document
                if(currentDocument == null) {
                    // New document
                    _atlas.UpdateDocument(viewDocument);
                } else {
                    if(DocumentComparer.Equals(viewDocument, currentDocument, true)) {
                        // Noop, change did not affect cached view document
                    } else {
                        _atlas.UpdateDocument(viewDocument);
                    }
                }
            }
        }
    }

    private Expression<Func<TDocument, bool>> BuildTriggerExpressionFromDocument(SqlServerViewSourceTableTrigger trigger, TDocument document) {
        Expression? body = null;
        var param = Expression.Parameter(typeof(TDocument), "document");
        foreach(var property in trigger.Fields) {
            var equalExpression = Expression.Equal(Expression.Property(param, property.Property), Expression.Constant(property.Property.GetValue(document)));
            body = body != null ? Expression.And(body, equalExpression) : equalExpression;
        }

        if(body == null)
            throw new ArgumentException($"{typeof(TDocument)} must have a trigger field");

        return Expression.Lambda<Func<TDocument, bool>>(body, param);
    }

    private Expression<Func<TDocument, bool>> BuildViewExpressionFromDocument(TDocument document) {
        var keyProperties = TypeHelpers.GetAtlasKeyProperties<TDocument>();
        Expression? body = null;
        var param = Expression.Parameter(typeof(TDocument), "document");
        foreach(var property in keyProperties) {
            var equalExpression = Expression.Equal(Expression.Property(param, property.Property), Expression.Constant(property.Property.GetValue(document)));
            body = body != null ? Expression.And(body, equalExpression) : equalExpression;
        }

        if(body == null)
            throw new ArgumentException($"{typeof(TDocument)} must have a key field");

        return Expression.Lambda<Func<TDocument, bool>>(body, param);
    }

    public override void Dispose() {
        _source.Dispose();
    }
}