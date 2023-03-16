namespace Fireflies.Atlas.Core.Helpers;

internal static class AtlasDocumentExtensions {
    public static int CalculateKey<TDocument>(this TDocument document) {
        var current = 0;

        foreach(var (property, _) in TypeHelpers.GetAtlasKeyProperties(typeof(TDocument))) {
            var value = property.GetValue(document);
            current = HashCode.Combine(current, value);
        }

        return current;
    }

    public static bool IsAllKeysAssigned<TDocument>(this TDocument document) {
        foreach(var (property, _) in TypeHelpers.GetAtlasKeyProperties(typeof(TDocument))) {
            var defaultValue = property.PropertyType.IsValueType ? Activator.CreateInstance(property.PropertyType) : null;
            var value = property.GetValue(document);
            if(value == null || value.Equals(defaultValue))
                return false;
        }

        return true;
    }

    public static string AsString<TDocument>(this TDocument document) {
        return string.Join(", ", TypeHelpers.GetAtlasProperties(typeof(TDocument)).Select(x => $"{x.Property.Name}={x.Property.GetValue(document)}"));
    }
}