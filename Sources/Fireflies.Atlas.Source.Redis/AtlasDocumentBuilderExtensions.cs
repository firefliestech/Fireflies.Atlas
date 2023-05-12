using Fireflies.Atlas.Core;

namespace Fireflies.Atlas.Source.Redis;

public static class AtlasDocumentBuilderExtensions {
    public static AtlasDocumentBuilder<TDocument> RedisHashSource<TDocument>(this AtlasDocumentBuilder<TDocument> builder, RedisSource source, int database, string key) where TDocument : new() {
        builder.AddSource(new RedisHashSource<TDocument>(source, new HashDescriptor { Database = database, Key = key }));
        return builder;
    }
}