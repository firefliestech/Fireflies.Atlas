using System.Collections.Concurrent;
using System.Linq.Expressions;
using System.Reflection;
using Fireflies.Atlas.Core.Delegate;
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

    public event DocumentLoaded<TDocument>? Loaded;
    public event DocumentUpdated<TDocument>? Updated;
    public event DocumentDeleted<TDocument>? Deleted;

    public AtlasDocumentDictionary(Atlas atlas) {
        _atlas = atlas;
        _logger = _atlas.LoggerFactory.GetLogger<AtlasDocumentDictionary<TDocument>>();
    }

    internal AtlasSource<TDocument> Source { get; set; } = null!;
    internal AtlasRelation<TDocument>[] Relations { get; set; } = null!;
    internal Func<TDocument, QueryContext, AtlasRelation<TDocument>[], TDocument> ProxyFactory { get; set; } = null!;

    internal async Task Preload() {
        _preloaded = true;
        _logger.Debug(() => "Preloading...");
        await LoadDocumentsFromSource(null, ExecutionFlags.None).ConfigureAwait(false);
        _logger.Trace(() => $"Preloading done! {_documents.Count} documents loaded.");
    }

    internal void UpdateDocument(TDocument document) {
        var updated = false;

        TDocument? oldDocument = default;
        var key = DocumentHelpers.CalculateKey(document);
        _documents.AddOrUpdate(key, _ => document, (_, oldValue) => {
            updated = true;
            oldDocument = oldValue;
            return document;
        });

        foreach(var index in _indexes)
            index.AddOrUpdate(document);

        if(updated) {
            _logger.Debug(() => $"Document was updated. New: {DocumentHelpers.AsString(document)}. Old: {DocumentHelpers.AsString(oldDocument)}");

            Updated?.Invoke(ProxyFactory(document, new QueryContext(), Relations),
                oldDocument != null ? ProxyFactory(oldDocument, new QueryContext(), Relations) : default);
        } else {
            Loaded?.Invoke(ProxyFactory(document, new QueryContext(), Relations));
        }
    }

    public void DeleteDocument(TDocument document) {
        var key = DocumentHelpers.CalculateKey(document);
        if(_documents.TryRemove(key, out _)) {
            _logger.Debug(() => $"Document was deleted. New: {DocumentHelpers.AsString(document)}");

            foreach(var index in _indexes)
                index.Remove(document);

            Deleted?.Invoke(document);
        }
    }

    public async Task<IEnumerable<TDocument>> GetDocuments(Expression predicate, QueryContext queryContext, CacheFlag cacheFlag, ExecutionFlags flags) {
        var whereAggregateVisitor = new WhereAggregateVisitor();
        var normalizedExpression = whereAggregateVisitor.CreateWhereExpression(predicate);

        if(queryContext.Contains(normalizedExpression)) {
            return queryContext.Get(normalizedExpression);
        }

        if(normalizedExpression != null && cacheFlag != CacheFlag.BypassCache) {
            // Create document and check for key in local cache if all populated
            if(TryGetKeyedQueryFromCache(queryContext, normalizedExpression, out var cachedResult))
                return cachedResult;
        }

        TDocument[] result;
        if(cacheFlag == CacheFlag.OnlyCache || (cacheFlag == CacheFlag.Default && _preloaded)) {
            // If preloaded, all documents should already be in memory
            if(_preloaded)
                _logger.Trace(() => $"Documents were preloaded. Searching in cache. Predicate: {normalizedExpression}");
            else
                _logger.Trace(() => $"{nameof(CacheFlag.OnlyCache)} was specified. Searching in cache, even if not not all documents might be loaded. Predicate: {normalizedExpression}");

            if(normalizedExpression == null) {
                result = _documents.Values.ToArray();
            } else {
                IEnumerable<TDocument> possibleDocuments = _documents.Values;
                if(TryLimitPossibleResultsByIndex(normalizedExpression, possibleDocuments, out var filteredDocuments)) {
                    possibleDocuments = filteredDocuments;
                    _logger.Trace(() => $"Documents were filtered by index. Documents found: {possibleDocuments.Count()}. Predicate: {normalizedExpression}");
                } else {
                    _logger.Warn(() => $"Documents will be found by SCAN. Predicate: {normalizedExpression}");
                }

                possibleDocuments = possibleDocuments.Where(ExpressionCompiler.Compile(normalizedExpression));
                result = possibleDocuments.ToArray();
            }

            if(_preloaded)
                _logger.Trace(() => $"Documents were preloaded. Searched in cache. Documents found: {result.Length}. Predicate: {normalizedExpression}");
            else
                _logger.Trace(() => $"{nameof(CacheFlag.OnlyCache)} was specified. Searched in cache. Documents found: {result.Length}. Predicate: {normalizedExpression}");
        } else {
            if(!_preloaded)
                _logger.Trace(() => $"Documents were not preloaded. Searching in source. Predicate: {normalizedExpression}");
            else
                _logger.Trace(() => $"Documents were preloaded but {nameof(CacheFlag.BypassCache)} was specified. Searching in source. Predicate: {normalizedExpression}");

            result = await LoadDocumentsFromSource(normalizedExpression, flags).ConfigureAwait(false);

            if(!_preloaded)
                _logger.Trace(() => $"Documents were not preloaded. Searching in source. Documents found: {result.Length}. Predicate: {normalizedExpression}");
            else
                _logger.Trace(() => $"Documents were preloaded but {nameof(CacheFlag.BypassCache)} was specified. Searched in source. PDocuments found: {result.Length}. Predicate: {normalizedExpression}");
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
        var allKeysAssigned = DocumentHelpers.IsAllKeysAssigned(queryDocument);
        if(!allKeysAssigned) {
            result = Enumerable.Empty<TDocument>();
            return false;
        }

        var key = DocumentHelpers.CalculateKey(queryDocument);
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

    private async Task<TDocument[]> LoadDocumentsFromSource(Expression<Func<TDocument, bool>>? predicate, ExecutionFlags flags) {
        _logger.Trace(() => $"Getting documents from source. Predicate: {predicate}");
        var documents = await Source.GetDocuments(predicate, flags).ConfigureAwait(false);

        var result = new List<TDocument>();
        foreach(var document in documents.ToArray()) {
            result.Add(document.Document);
            if(document.Cache) {
                UpdateDocument(document.Document);
            } else {
                DeleteDocument(document.Document);
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

    public override void Dispose() {
        Source.Dispose();
    }

    public async Task TriggerUpdate(Expression<Func<TDocument, bool>> predicate) {
        foreach(var affectedDocument in await GetDocuments(predicate, new QueryContext(), CacheFlag.OnlyCache, ExecutionFlags.None).ConfigureAwait(false))
            Updated?.Invoke(affectedDocument, affectedDocument);
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

        protected override Expression VisitMember(MemberExpression node) {
            if(node.Expression is ConstantExpression constantExpression) {
                if(node.Member is PropertyInfo propertyInfo) {
                    var value = propertyInfo.GetValue(constantExpression.Value);
                    return Expression.Constant(value, node.Type);
                }

                if(node.Member is FieldInfo fieldInfo) {
                    var value = fieldInfo.GetValue(constantExpression.Value);
                    return Expression.Constant(value, node.Type);
                }

                throw new NotSupportedException($"Constant expressions with member type '{node.Member.MemberType}' is not supported");
            }

            if(node.Expression is MemberExpression memberExpression) {
                var subNode = VisitMember(memberExpression);
                if(subNode is ConstantExpression constantSubExpression) {
                    return Visit(node.Update(constantSubExpression));
                }

                return Visit(subNode);
            }

            return base.VisitMember(node);
        }

        protected override Expression VisitBinary(BinaryExpression node) {
            if(node is { Left: ConstantExpression } and not { Right: ConstantExpression }) {
                node = Expression.MakeBinary(node.NodeType, node.Right, node.Left);
            }

            var left = Visit(node.Left);
            var leftIsNullableType = Nullable.GetUnderlyingType(left.Type) != null;

            var right = Visit(node.Right);
            var rightIsNullableType = Nullable.GetUnderlyingType(right.Type) != null;

            return leftIsNullableType switch {
                true when !rightIsNullableType => node.Update(left, VisitAndConvert(node.Conversion, nameof(VisitBinary)), Expression.Convert(right, left.Type)),
                false when rightIsNullableType => node.Update(Expression.Convert(left, right.Type), VisitAndConvert(node.Conversion, nameof(VisitBinary)), right),
                _ => node.Update(left, VisitAndConvert(node.Conversion, nameof(VisitBinary)), right)
            };
        }
    }
}