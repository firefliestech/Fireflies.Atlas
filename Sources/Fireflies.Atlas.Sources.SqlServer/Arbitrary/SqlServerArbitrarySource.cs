using System.Linq.Expressions;
using Fireflies.Atlas.Core;
using Fireflies.Atlas.Core.Helpers;
using Fireflies.Atlas.Sources.SqlServer.Arbitrary.View;
using Fireflies.Logging.Abstractions;

namespace Fireflies.Atlas.Sources.SqlServer.Arbitrary;

public abstract class SqlServerArbitrarySource<TDocument> : AtlasSource<TDocument> where TDocument : new() {
    private readonly Core.Atlas _atlas;
    private readonly SqlServerSource _source;
    private readonly SqlDescriptor _viewDescriptor;
    private readonly Expression<Func<TDocument, bool>>? _filterExpression;
    private readonly Func<TDocument, bool>? _compiledFilter;
    private readonly IFirefliesLogger _logger;
    private readonly bool _cacheEnabled;

    protected SqlServerArbitrarySource(Core.Atlas atlas, SqlServerSource source, SqlDescriptor viewDescriptor, SqlServerArbitrarySourceBuilder<TDocument> builder) {
        _atlas = atlas;
        _source = source;
        _viewDescriptor = viewDescriptor;
        _filterExpression = builder.Filter;
        _compiledFilter = _filterExpression?.Compile();
        _cacheEnabled = builder.CacheEnabled;
        _logger = atlas.LoggerFactory.GetLogger<SqlServerViewSource<TDocument>>();

        var tableTriggers = builder.TableTriggerBuilders.Select(x => x.Build());
        foreach(var trigger in tableTriggers) {
            var monitor = _source.UpdateFetcher.MonitorTable<TDocument>(trigger.SqlDescriptor);

            monitor.Deleted += deletedDocument => MonitorOnDeleted(trigger, deletedDocument);
            monitor.Inserted += insertedDocument => MonitorOnInserted(trigger, insertedDocument);
            monitor.Updated += (updatedDocument, deletedDocument) => MonitorOnUpdated(trigger, updatedDocument, deletedDocument);
        }
    }

    public override void Validate(AtlasDocumentBuilder atlasBuilder) {
        if(atlasBuilder.Preload && !_cacheEnabled)
            throw new AtlasException($"If {nameof(atlasBuilder.Preload)} is enabled cache cant´t be disabled");
    }

    public override async Task<IEnumerable<(bool Cache, TDocument Document)>> GetDocuments(Expression<Func<TDocument, bool>>? predicate, ExecutionFlags flags) {
        var result = await _source.GetDocuments(predicate, _viewDescriptor, _filterExpression, flags).ConfigureAwait(false);
        if(_compiledFilter == null || !_cacheEnabled)
            return result.Select(x => (_cacheEnabled, x));

        return result.Select(x => (_compiledFilter(x), x));
    }

    private async void MonitorOnUpdated(SqlServerArbitrarySourceTableTrigger trigger, TDocument newDocument, Lazy<TDocument> deletedDocument) {
        _logger.Trace(() => $"Document was updated. Trigger: {trigger.SqlDescriptor}. New: {DocumentHelpers.AsString(newDocument)}. Old: {DocumentHelpers.AsString(deletedDocument.Value)}");
        var triggerExpression = BuildTriggerExpressionFromDocument(trigger, deletedDocument.Value);
        var documentsThatNeedsToBeReloaded = await _atlas.GetDocuments(triggerExpression, CacheFlag.OnlyCache).ConfigureAwait(false);
        HandleUpsert(documentsThatNeedsToBeReloaded);
    }

    private async void MonitorOnInserted(SqlServerArbitrarySourceTableTrigger trigger, TDocument newDocument) {
        _logger.Trace(() => $"Document was inserted. Trigger: {trigger.SqlDescriptor}. New: {DocumentHelpers.AsString(newDocument)}");
        if(trigger.HasKeyField) {
            await UpdateDocument(newDocument).ConfigureAwait(false);
            return;
        }

        var triggerExpression = BuildTriggerExpressionFromDocument(trigger, newDocument);
        var documentsThatNeedsToBeReloaded = await _atlas.GetDocuments(triggerExpression, CacheFlag.OnlyCache).ConfigureAwait(false);
        HandleUpsert(documentsThatNeedsToBeReloaded);
    }

    private async void MonitorOnDeleted(SqlServerArbitrarySourceTableTrigger trigger, TDocument deletedDocument) {
        _logger.Trace(() => $"Document was deleted. Trigger: {trigger.SqlDescriptor}. Deleted: {DocumentHelpers.AsString(deletedDocument)}");
        var queryExpression = BuildTriggerExpressionFromDocument(trigger, deletedDocument);
        var currentDocument = await _atlas.GetDocument(queryExpression, CacheFlag.OnlyCache).ConfigureAwait(false);
        if(currentDocument != null)
            await UpdateDocument(currentDocument).ConfigureAwait(false);
    }

    private async void HandleUpsert(IEnumerable<TDocument> documentsThatNeedsToBeReloaded) {
        foreach(var currentDocument in documentsThatNeedsToBeReloaded) {
            await UpdateDocument(currentDocument).ConfigureAwait(false);
        }
    }

    private async Task UpdateDocument(TDocument currentDocument) {
        var keyQuery = DocumentHelpers.BuildKeyExpressionFromDocument(currentDocument);
        var viewDocuments = await GetDocuments(keyQuery, ExecutionFlags.None).ConfigureAwait(false);
        foreach(var entry in viewDocuments) {
            var viewDocument = entry.Document;
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

            break;
        }
    }

    private Expression<Func<TDocument, bool>> BuildTriggerExpressionFromDocument(SqlServerArbitrarySourceTableTrigger trigger, TDocument document) {
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

    public override void Dispose() {
        _source.Dispose();
    }
}