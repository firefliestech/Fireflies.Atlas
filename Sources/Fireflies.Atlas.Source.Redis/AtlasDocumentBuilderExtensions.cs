using Fireflies.Atlas.Annotations;
using Fireflies.Atlas.Core;
using Fireflies.Atlas.Core.Helpers;

namespace Fireflies.Atlas.Source.Redis;

public static class AtlasDocumentBuilderExtensions {
    public static AtlasDocumentBuilder<TDocument> RedisHashSource<TDocument>(this AtlasDocumentBuilder<TDocument> builder, RedisSource source, int database, string key) where TDocument : new() {
        var keyProperties = TypeHelpers.GetAtlasKeyProperties(typeof(TDocument));
        switch(keyProperties.Count()) {
            case 0:
                throw new ArgumentException($"{typeof(TDocument).Name} needs to have field with a {nameof(AtlasKeyAttribute)}");
            case > 1:
                throw new ArgumentException($"{typeof(TDocument).Name} must only have one {nameof(AtlasKeyAttribute)} property");
        }

        builder.AddSource(new RedisHashSource<TDocument>(source, new HashDescriptor { Database = database, Key = key, KeyProperty = keyProperties.First().Property }));
        
        return builder;
    }

    public static AtlasDocumentBuilder<TDocument> RedisKeySource<TDocument>(this AtlasDocumentBuilder<TDocument> builder, RedisSource source, int database, Func<string, string> keyBuilder) where TDocument : new() {
        var properties = TypeHelpers.GetAtlasProperties<TDocument>();
        var keyProperties = properties.Where(x => x.Attributes.Any(a => a.GetType().IsAssignableTo(typeof(AtlasKeyAttribute))));
        switch(keyProperties.Count()) {
            case 0:
                throw new ArgumentException($"{typeof(TDocument).Name} needs to have field with a {nameof(AtlasKeyAttribute)}");
            case > 1:
                throw new ArgumentException($"{typeof(TDocument).Name} must only have one {nameof(AtlasKeyAttribute)} property");
        }

        var valueProperties = properties.Where(x => x.Attributes.Any(a => a.GetType().IsAssignableTo(typeof(AtlasFieldAttribute))) && x.Attributes.All(a => !a.GetType().IsAssignableTo(typeof(AtlasKeyAttribute))));
        switch(valueProperties.Count()) {
            case 0:
                throw new ArgumentException($"{typeof(TDocument).Name} needs to have field with a {nameof(AtlasFieldAttribute)} which does not also have a {nameof(AtlasKeyAttribute)} attribute");
            case > 1:
                throw new ArgumentException($"{typeof(TDocument).Name} must only have one {nameof(AtlasFieldAttribute)} property");
        }

        builder.AddSource(new RedisKeySource<TDocument>(source, new KeyDescriptor { Database = database, KeyBuilder = keyBuilder, KeyProperty = keyProperties.First().Property, ValueProperty = valueProperties.First().Property }));
        return builder;
    }
}