﻿using System.Collections.Concurrent;
using System.Linq.Expressions;
using Fireflies.Logging.Abstractions;

namespace Fireflies.Atlas.Core;

public class Atlas : IDisposable {
    private readonly ConcurrentDictionary<Type, AtlasDocumentDictionary> _dictionaries = new();
    private IFirefliesLoggerFactory _loggerFactory = new NullLoggerFactory();
    private IFirefliesLogger _logger;

    internal IEnumerable<AtlasDocumentDictionary> DocumentDictionaries => _dictionaries.Values;

    public Atlas() {
        _logger = _loggerFactory.GetLogger<Atlas>();
    }

    public IFirefliesLoggerFactory LoggerFactory {
        get => _loggerFactory;
        internal set {
            _loggerFactory = value;
            _logger = _loggerFactory.GetLogger<Atlas>();
        }
    }

    public Task<TDocument?> GetDocument<TDocument>(Expression<Func<TDocument, bool>> predicate, ExecutionFlags flags = ExecutionFlags.None) where TDocument : new() {
        return GetDocument<TDocument>(predicate, new QueryContext(), flags);
    }

    public Task<TDocument?> GetDocument<TDocument>(Expression predicate, ExecutionFlags flags = ExecutionFlags.None) where TDocument : new() {
        return GetDocument<TDocument>(predicate, new QueryContext(), flags);
    }

    public Task<IEnumerable<TDocument>> GetDocuments<TDocument>(ExecutionFlags flags = ExecutionFlags.None) where TDocument : new() {
        return GetDocuments<TDocument>(x => true, flags);
    }

    public Task<IEnumerable<TDocument>> GetDocuments<TDocument>(Expression<Func<TDocument, bool>> predicate, ExecutionFlags flags = ExecutionFlags.None) where TDocument : new() {
        return GetDocuments<TDocument>((Expression)predicate, flags);
    }

    public async Task<IEnumerable<TDocument>> GetDocuments<TDocument>(Expression predicate, ExecutionFlags flags = ExecutionFlags.None) where TDocument : new() {
        var startedAt = DateTimeOffset.UtcNow;

        var result = await GetDocuments<TDocument>(predicate, new QueryContext(), flags).ConfigureAwait(false);
        _logger.Trace(() => $"Got {result.Count()} documents in {(DateTimeOffset.UtcNow - startedAt).TotalMilliseconds}ms. Predicate: {predicate}");

        return result;
    }

    internal async Task<TDocument?> GetDocument<TDocument>(Expression predicate, QueryContext queryContext, ExecutionFlags flags = ExecutionFlags.None) where TDocument : new() {
        var result = await GetDocuments<TDocument>(predicate, queryContext, flags).ConfigureAwait(false);
        return result.FirstOrDefault();
    }

    internal Task<IEnumerable<TDocument>> GetDocuments<TDocument>(Expression predicate, QueryContext queryContext, ExecutionFlags flags = ExecutionFlags.None) where TDocument : new() {
        var dictionary = InternalGetDictionary<TDocument>();
        var result = dictionary == null ? Task.FromResult(Enumerable.Empty<TDocument>()) : dictionary.GetDocuments(predicate, queryContext, flags);
        return result;
    }

    internal AtlasDocumentDictionary<TDocument> Add<TDocument>() where TDocument : new() {
        return (AtlasDocumentDictionary<TDocument>)_dictionaries.GetOrAdd(typeof(TDocument), new AtlasDocumentDictionary<TDocument>(this));
    }

    public void UpdateDocument<TDocument>(TDocument document) where TDocument : new() {
        InternalGetDictionary<TDocument>()?.UpdateDocument(document);
    }

    public void DeleteDocument<TDocument>(TDocument document) where TDocument : new() {
        InternalGetDictionary<TDocument>()?.DeleteDocument(document);
    }

    public IDocumentDictionary<TDocument>? GetDictionary<TDocument>() where TDocument : new() {
        return InternalGetDictionary<TDocument>();
    }

    private AtlasDocumentDictionary<TDocument>? InternalGetDictionary<TDocument>() where TDocument : new() {
        if(!_dictionaries.TryGetValue(typeof(TDocument), out var dictionary))
            return null;

        var castDictionary = (AtlasDocumentDictionary<TDocument>)dictionary;
        return castDictionary;
    }

    public void Dispose() {
        foreach(var x in _dictionaries.Values) {
           x.Dispose();
        }
    }

    public async Task TriggerUpdate<TDocument>(Expression<Func<TDocument, bool>> predicate) where TDocument : new() {
        var documentDictionary = InternalGetDictionary<TDocument>();
        if(documentDictionary != null)
            await documentDictionary.TriggerUpdate(predicate).ConfigureAwait(false);
    }
}