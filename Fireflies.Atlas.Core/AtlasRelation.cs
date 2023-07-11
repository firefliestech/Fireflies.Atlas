using System.Linq.Expressions;
using System.Reflection;

namespace Fireflies.Atlas.Core;

public abstract class AtlasRelation<TDocument> {
    public abstract Task<object?> GetForeignDocument(TDocument document, QueryContext queryContext);
    public abstract string PropertyName { get; }
    public PropertyInfo Property { get; protected set; }
}

public class AtlasRelation<TDocument, TForeign> : AtlasRelation<TDocument> where TForeign : new() where TDocument : new() {
    private readonly Atlas _atlas;
    private readonly Expression<Func<TDocument, TForeign, bool>> _matchExpression;

    private readonly PropertyInfo _propertyInfo;
    public override string PropertyName { get; }

    public AtlasRelation(Atlas atlas, Expression<Func<TDocument, object>> property, Expression<Func<TDocument, TForeign, bool>> matchExpression) {
        _atlas = atlas;
        _matchExpression = matchExpression;
        _propertyInfo = (PropertyInfo)((MemberExpression)property.Body).Member;
        Property = _propertyInfo;
        PropertyName = _propertyInfo.GetMethod.Name;
    }

    public override async Task<object?> GetForeignDocument(TDocument document, QueryContext queryContext) {
        var replacedExpression = ReplaceMemberAccessWithConstant(_matchExpression, _matchExpression.Parameters.First(), document);
        if (!replacedExpression.Success) {
            return default;
        }

        var queryExpression = Expression.Lambda<Func<TForeign, bool>>(replacedExpression.Expression, _matchExpression.Parameters.Last());
        object? foreign;
        if (_propertyInfo.PropertyType.IsAssignableTo(typeof(IEnumerable<TForeign>))) {
            foreign = await _atlas.GetDocuments<TForeign>(queryExpression, queryContext).ConfigureAwait(false);
        } else {
            foreign = await _atlas.GetDocument<TForeign>(queryExpression, queryContext).ConfigureAwait(false);
        }

        return foreign;
    }

    private (bool Success, Expression Expression) ReplaceMemberAccessWithConstant(Expression expression, ParameterExpression documentParameter, TDocument document) {
        if (expression is LambdaExpression lambdaExpression) {
            // Start of by analyzing the lambda
            return ReplaceMemberAccessWithConstant(lambdaExpression.Body, documentParameter, document);
        } else if (expression is BinaryExpression { NodeType: ExpressionType.Equal } binaryExpression) {
            // Replace the childs of the the two equal branches
            var left = ReplaceMemberAccessWithConstant(binaryExpression.Left, documentParameter, document);
            var right = ReplaceMemberAccessWithConstant(binaryExpression.Right, documentParameter, document);
            return (left.Success && right.Success, Expression.Equal(left.Expression, right.Expression));
        } else if (expression is MemberExpression memberExpression && memberExpression.Expression == documentParameter) {
            // Replace the actual value. If the type is nullable we always convert the value to correct type even if the value is not null
            var lambda = Expression.Lambda<Func<TDocument, object?>>(Expression.Convert(memberExpression, typeof(object)), documentParameter);
            var valueDelegate = ExpressionCompiler.Compile(lambda);
            var value = valueDelegate(document);
            if (Nullable.GetUnderlyingType(memberExpression.Type) != null)
                return (value != null, Expression.Convert(Expression.Constant(value), memberExpression.Type));
            return (value != null, Expression.Constant(value));
        } else if (expression is UnaryExpression { NodeType: ExpressionType.Convert } unaryExpression) {
            // If there is a convert node (used if matching nullable with non-nullable types)
            var replaceMemberAccessWithConstant = ReplaceMemberAccessWithConstant(unaryExpression.Operand, documentParameter, document);
            return (replaceMemberAccessWithConstant.Success, Expression.MakeUnary(unaryExpression.NodeType, replaceMemberAccessWithConstant.Expression, unaryExpression.Type));
        } else {
            // Otherwise just return the current expression
            return (true, expression);
        }
    }
}