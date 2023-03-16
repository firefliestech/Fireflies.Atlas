using System.Collections.Concurrent;
using System.Linq.Expressions;
using Fireflies.Atlas.Annotations;
using Fireflies.Atlas.Core.Helpers;
using Fireflies.Logging.Abstractions;

namespace Fireflies.Atlas.Core;

public abstract class AtlasDocumentDictionary : IDisposable {
    public abstract void Dispose();
}

public class AtlasDocumentDictionary<TDocument> : AtlasDocumentDictionary, IDocumentDictionary<TDocument> where TDocument : new() {
    private readonly Atlas _atlas;

    private readonly ConcurrentDictionary<int, TDocument> _documents = new();

    private readonly IFirefliesLogger _logger;
    private bool _preloaded;
    private readonly List<FieldIndex<TDocument>> _indexes = new();

    public event Action<TDocument>? Loaded;
    public event Action<TDocument>? Updated;
    public event Action<TDocument>? Deleted;

    public AtlasDocumentDictionary(Atlas atlas) {
        _atlas = atlas;
        _logger = _atlas.LoggerFactory.GetLogger<AtlasDocumentDictionary<TDocument>>();
    }

    internal AtlasSource Source { get; set; } = null!;
    internal AtlasRelation<TDocument>[] Relations { get; set; } = null!;
    internal Func<TDocument, QueryContext, AtlasRelation<TDocument>[], TDocument> ProxyFactory { get; set; } = null!;

    internal async Task Preload() {
        _preloaded = true;
        _logger.Debug(() => "Preloading...");
        await InternalGetDocuments();
        _logger.Trace(() => $"Preloading done! {_documents.Count} documents loaded.");
    }

    public void UpdateDocument(TDocument document) {
        var updated = false;

        TDocument oldDocument = default;
        var key = document.CalculateKey();
        _documents.AddOrUpdate(key, _ => document, (_, oldValue) => {
            updated = !DocumentComparer.Equals(document, oldValue);
            oldDocument = oldValue;
            return document;
        });

        if(updated) {
            foreach(var index in _indexes) {
                index.Remove(document);
            }
        }

        foreach(var index in _indexes) {
            index.Add(document);
        }

        if(updated) {
            _logger.Debug(() => $"Document was updated. New: {document.AsString()}. Old: {oldDocument.AsString()}");
            Updated?.Invoke(document);
        } else {
            Loaded?.Invoke(document);
        }
    }

    public void DeleteDocument(TDocument document) {
        var key = document.CalculateKey();
        if(_documents.TryRemove(key, out _)) {
            Deleted?.Invoke(document);
        }
    }

    public async Task<IEnumerable<TDocument>> GetDocuments(Expression predicate, QueryContext queryContext) {
        var whereAggregateVisitor = new WhereAggregateVisitor();
        var normalizedExpression = whereAggregateVisitor.CreateWhereExpression(predicate);

        if(queryContext.Contains(normalizedExpression)) {
            return queryContext.Get(normalizedExpression);
        }

        if(normalizedExpression != null) {
            // Create document and check for key in local cache if all populated
            if(TryGetKeyedQueryFromCache(queryContext, normalizedExpression, out var cachedResult))
                return cachedResult;
        }

        TDocument[] result;
        if(_preloaded) {
            // If preloaded, all documents should already be in memory
            _logger.Trace(() => $"Documents were preloaded. Searching in cache. Predicate: {normalizedExpression}");

            if(normalizedExpression == null) {
                result = _documents.Values.ToArray();
            } else {
                IEnumerable<TDocument> possibleDocuments = _documents.Values;
                if(TryLimitPossibleResultsByIndex(normalizedExpression, possibleDocuments, out var filteredDocuments)) {
                    possibleDocuments = filteredDocuments;
                    _logger.Trace(() => $"Documents were filtered by index. Documents found: {possibleDocuments.Count()}. Predicate: {predicate}");
                } else {
                    _logger.Warn(() => $"Documents will be found by SCAN. Predicate: {predicate}");
                }

                possibleDocuments = possibleDocuments.Where(ExpressionCompiler.Compile(normalizedExpression));
                result = possibleDocuments.ToArray();
            }

            _logger.Trace(() => $"Documents were preloaded. Searching in cache. Documents found: {result.Length}. Predicate: {normalizedExpression}");
        } else {
            _logger.Trace(() => $"Documents were not preloaded. Searching in source. Predicate: {predicate}");
            result = await InternalGetDocuments(normalizedExpression);
            _logger.Trace(() => $"Documents were not preloaded. Searching in source. Documents found: {result.Length}. Predicate: {normalizedExpression}");
        }

        return queryContext.Add(normalizedExpression, CreateProxies(result, queryContext));
    }

    private bool TryLimitPossibleResultsByIndex(Expression<Func<TDocument, bool>> normalizedExpression, IEnumerable<TDocument> possibleDocuments, out IEnumerable<TDocument> result) {
        result = possibleDocuments;

        var success = false;
        foreach(var index in _indexes) {
            var localResult = index.Match(normalizedExpression);
            if(localResult.Success) {
                result = success ? result.Intersect(localResult.RemainingDocuments) : localResult.RemainingDocuments;
                success = true;
            }
        }

        return success;
    }

    private bool TryGetKeyedQueryFromCache(QueryContext queryContext, Expression<Func<TDocument, bool>> normalizedExpression, out IEnumerable<TDocument> result) {
        var queryDocument = PredicateToDocument.CreateDocument(normalizedExpression);
        var allKeysAssigned = queryDocument.IsAllKeysAssigned();
        if(!allKeysAssigned) {
            result = Enumerable.Empty<TDocument>();
            return false;
        }

        var key = queryDocument.CalculateKey();
        if(_documents.TryGetValue(key, out var resultDocument)) {
            // Even if we match by key there could be additional queries not matching the keyed document, hence the Where clause
            var predicate = ExpressionCompiler.Compile(normalizedExpression);
            var documents = new[] { resultDocument }.Where(predicate).ToArray();
            if(documents.Length > 0) {
                _logger.Trace(() => $"Query by key and document was found in cache and predicate was matching. Predicate: {normalizedExpression}");
            } else {
                _logger.Warn(() => $"Query by key and document was found in cache but predicate was NOT matching. Predicate: {normalizedExpression}");
            }

            var proxiedDocuments = CreateProxies(documents, queryContext);
            {
                result = queryContext.Add(normalizedExpression, proxiedDocuments);
                return true;
            }
        }

        _logger.Trace(() => $"Query by key but document was NOT found in cache. Predicate: {normalizedExpression}");

        result = Enumerable.Empty<TDocument>();
        return false;
    }

    private async Task<TDocument[]> InternalGetDocuments(Expression<Func<TDocument, bool>>? predicate = null) {
        _logger.Trace(() => $"Getting documents from source. Predicate: {predicate}");
        var (cache, documents) = await Source.GetDocuments(predicate);

        var result = new List<TDocument>();
        foreach(var document in documents.ToArray()) {
            result.Add(document);
            if(cache) {
                UpdateDocument(document);
            }
        }

        var results = result.ToArray();
        _logger.Trace(() => $"Got {results.Length} documents from source. Predicate: {predicate}");
        return results;
    }

    private TDocument[] CreateProxies(TDocument[] atlasDocuments, QueryContext queryContext) {
        var result = new TDocument[atlasDocuments.Length];
        for(var i = 0; i < atlasDocuments.Length; i++) {
            var document = atlasDocuments[i];
            var proxiedDocument = ProxyFactory(document, queryContext, Relations);
            result[i] = proxiedDocument;
        }

        return result;
    }

    public void AddIndex<TProperty>(Expression<Func<TDocument, TProperty>> property) {
        _indexes.Add(new FieldIndex<TDocument, TProperty>(property));
    }

    private class WhereAggregateVisitor : ExpressionVisitor {
        private Expression? _combinedExpression;

        private readonly ParameterExpression _parameterExpression = Expression.Parameter(typeof(TDocument), "document");
        private int _whereCounter;

        public Expression<Func<TDocument, bool>>? CreateWhereExpression(Expression expression) {
            if(expression is LambdaExpression) {
                _whereCounter++;
            }

            Visit(expression);
            if(_combinedExpression == null)
                return null;

            return Expression.Lambda<Func<TDocument, bool>>(_combinedExpression ?? Expression.Constant(true), _parameterExpression);
        }

        protected override Expression VisitParameter(ParameterExpression node) {
            return _parameterExpression;
        }

        protected override Expression VisitMethodCall(MethodCallExpression node) {
            if(node.Method.DeclaringType != typeof(Queryable))
                return base.VisitMethodCall(node);

            var isReducing = node.Method.Name is nameof(Queryable.Where)
                or nameof(Queryable.First)
                or nameof(Queryable.FirstOrDefault)
                or nameof(Queryable.Single)
                or nameof(Queryable.SingleOrDefault)
                or nameof(Queryable.Any);

            if(!isReducing)
                return base.VisitMethodCall(node);

            _whereCounter++;
            base.VisitMethodCall(node);
            _whereCounter--;
            return node;
        }

        protected override Expression VisitLambda<T>(Expression<T> node) {
            if(_whereCounter > 0) {
                _combinedExpression = _combinedExpression == null ? Visit(node.Body) : Expression.MakeBinary(ExpressionType.AndAlso, _combinedExpression, Visit(node.Body));
            }

            return node;
        }

        protected override Expression VisitBinary(BinaryExpression node) {
            if(node is { Left: ConstantExpression } and not { Right: ConstantExpression }) {
                node = Expression.MakeBinary(node.NodeType, node.Right, node.Left);
            }

            return node.Update(Visit(node.Left),
                VisitAndConvert(node.Conversion, nameof(VisitBinary)),
                Visit(node.Right));
        }
    }

    public override void Dispose() {
        Source.Dispose();
    }
}