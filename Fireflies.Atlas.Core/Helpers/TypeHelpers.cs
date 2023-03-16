using System.Collections.Concurrent;
using System.Reflection;
using Fireflies.Atlas.Annotations;

namespace Fireflies.Atlas.Core.Helpers;

public static class TypeHelpers {
    private static readonly ConcurrentDictionary<Type, IEnumerable<(PropertyInfo Property, AtlasKeyAttribute Attributes)>> _cachedAtlasKeyProperties = new();
    private static readonly ConcurrentDictionary<Type, IEnumerable<(PropertyInfo Property, IEnumerable<AtlasAttribute> Attributes)>> _cachedAtlasProperties = new();

    public static IEnumerable<(PropertyInfo Property, IEnumerable<TAttribute> Attributes)> GetAtlasProperties<T, TAttribute>(Func<TAttribute, bool> filter) {
        var atlasProperties = GetAtlasProperties(typeof(T));
        var withMatchingAttributes = atlasProperties
            .Where(x => x.Attributes.Any(a => a.GetType().IsAssignableTo(typeof(TAttribute))));
        return withMatchingAttributes
            .Select(x => new { x.Property, Attributes = x.Attributes.Where(a => a is TAttribute attribute && filter(attribute)).Cast<TAttribute>() })
            .Where(x => x.Attributes.Any())
            .Select(x => (x.Property, x.Attributes));
    }

    public static IEnumerable<(PropertyInfo Property, IEnumerable<AtlasAttribute> Attributes)> GetAtlasProperties<T>() {
        return GetAtlasProperties(typeof(T));
    }

    public static IEnumerable<(PropertyInfo Property, IEnumerable<AtlasAttribute> Attributes)> GetAtlasProperties(Type type) {
        return _cachedAtlasProperties.GetOrAdd(type, _ => {
            return type.GetProperties().Select(x => {
                var atlasAttributes = x.GetCustomAttributes(true).Where(a => a is AtlasAttribute).Cast<AtlasAttribute>();
                return (x, atlasAttributes);
            }).Where(x => x.atlasAttributes.Any());
        });
    }

    public static TSource? GetAttribute<TSource>(Type type) {
        return type.GetCustomAttributes(true).OfType<TSource>().FirstOrDefault();
    }

    public static IEnumerable<(PropertyInfo Property, AtlasKeyAttribute Attributes)> GetAtlasKeyProperties(Type type) {
        return _cachedAtlasKeyProperties.GetOrAdd(type, _ => {
            return type.GetProperties().Select(x =>
                (x, x.GetCustomAttributes(typeof(AtlasKeyAttribute), true).Cast<AtlasKeyAttribute>().FirstOrDefault())
            ).Where(x => x.Item2 != null)!;
        });
    }
}