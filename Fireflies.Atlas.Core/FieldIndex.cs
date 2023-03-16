using System.Collections.Concurrent;
using System.Linq.Expressions;

namespace Fireflies.Atlas.Core;

public class FieldIndex<TDocument, TProperty> : FieldIndex<TDocument> {
    private readonly Func<TDocument, TProperty> _keyAccessor;
    private readonly ConcurrentDictionary<TProperty, List<TDocument>> _indexedDocuments = new();

    public FieldIndex(Expression<Func<TDocument, TProperty>> property) {
        MemberExpression = (MemberExpression)property.Body;
        _keyAccessor = ExpressionCompiler.Compile(property);
    }

    public override object GetValue(TDocument document) {
        return _keyAccessor(document);
    }

    public override void Add(TDocument document) {
        var key = _keyAccessor(document);
        if (key == null)
            return;
        var list = _indexedDocuments.GetOrAdd(key, new List<TDocument>());
        list.Add(document);
    }

    public override void Remove(TDocument document) {
        var key = _keyAccessor(document);
        if (_indexedDocuments.TryGetValue(key, out var list)) {
            list.Remove(document);
        }
    }

    public override (bool Success, IEnumerable<TDocument> RemainingDocuments) Match(Expression<Func<TDocument, bool>> normalizedExpression) {
        var visitor = new GetIndexValueVisitor(MemberExpression);
        visitor.Visit(normalizedExpression);
        if (visitor.Success) {
            return _indexedDocuments.TryGetValue(visitor.Value, out var list) ? (true, list) : (true, Enumerable.Empty<TDocument>());
        }

        return (false, Enumerable.Empty<TDocument>());
    }

    private class GetIndexValueVisitor : ExpressionVisitor {
        private readonly MemberExpression _lookingFor;
        public bool Success { get; private set; }
        public TProperty Value { get; private set; }

        public GetIndexValueVisitor(MemberExpression lookingFor) {
            _lookingFor = lookingFor;
        }

        public override Expression? Visit(Expression? node) {
            if (Success)
                return node;

            return base.Visit(node);
        }

        protected override Expression VisitBinary(BinaryExpression node) {
            if (node is { NodeType: ExpressionType.Equal }) {
                var memberAndConstant = ExpressionHelper.GetMemberAndConstant(node);
                if (memberAndConstant != null) {
                    var y = memberAndConstant.Value;
                    if (y.MemberExpression.Member == _lookingFor.Member) {
                        Value = (TProperty)y.ConstantExpression.Value;
                        Success = true;
                    }
                }
            }

            return base.VisitBinary(node);
        }
    }
}

public abstract class FieldIndex<TDocument> {
    public MemberExpression MemberExpression { get; protected set; }
    public abstract object GetValue(TDocument document);

    public abstract void Add(TDocument document);
    public abstract void Remove(TDocument document);

    public abstract (bool Success, IEnumerable<TDocument> RemainingDocuments) Match(Expression<Func<TDocument, bool>> normalizedExpression);
}