using System.Linq.Expressions;

namespace Fireflies.Atlas.Core.Helpers;

public static class DocumentHelpers {
    public static int CalculateKey<TDocument>(TDocument document) {
        var current = 0;

        foreach(var (property, _) in TypeHelpers.GetAtlasKeyProperties(typeof(TDocument))) {
            var value = property.GetValue(document);
            current = HashCode.Combine(current, value);
        }

        return current;
    }

    public static bool IsAllKeysAssigned<TDocument>(TDocument document) {
        foreach(var (property, _) in TypeHelpers.GetAtlasKeyProperties(typeof(TDocument))) {
            var defaultValue = property.PropertyType.IsValueType ? Activator.CreateInstance(property.PropertyType) : null;
            var value = property.GetValue(document);
            if(value == null || value.Equals(defaultValue))
                return false;
        }

        return true;
    }

    public static string AsString<TDocument>(TDocument document) {
        return string.Join(", ", TypeHelpers.GetAtlasProperties(typeof(TDocument)).Select(x => $"{x.Property.Name}={x.Property.GetValue(document)}"));
    }

    public static Expression<Func<TDocument, bool>> BuildKeyExpressionFromDocument<TDocument>(TDocument document) {
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
}