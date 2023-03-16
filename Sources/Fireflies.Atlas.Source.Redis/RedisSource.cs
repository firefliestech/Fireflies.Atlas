using System.Collections.Concurrent;
using System.Linq.Expressions;
using System.Reflection;
using System.Text.Json;
using Fireflies.Atlas.Annotations;
using Fireflies.Atlas.Core;
using Fireflies.Atlas.Core.Helpers;
using StackExchange.Redis;

namespace Fireflies.Atlas.Source.Redis;

public class RedisSource {
    private readonly Core.Atlas _atlas;
    private readonly ConnectionMultiplexer _redis;
    private readonly ConcurrentDictionary<Type, PropertyInfo> _propertyCache = new();

    public RedisSource(Core.Atlas atlas, string connectionString) {
        _atlas = atlas;
        _redis = ConnectionMultiplexer.Connect(connectionString);
    }

    public Task<(bool Cache, IEnumerable<TDocument> Documents)> GetDocuments<TDocument>(Expression<Func<TDocument, bool>>? predicate, HashDescriptor hashDescriptor) where TDocument : new() {
        var documents = InternalGetDocuments(predicate, hashDescriptor);
        return Task.FromResult((false, documents));
    }

    private IEnumerable<TDocument> InternalGetDocuments<TDocument>(Expression<Func<TDocument, bool>>? predicate, HashDescriptor hashDescriptor) where TDocument : new() {
        var key = GetKey(typeof(TDocument));

        var queryDocument = PredicateToDocument.CreateDocument(predicate);
        var keyValue = key.GetMethod!.Invoke(queryDocument, Array.Empty<object>())?.ToString();
        if(keyValue == null)
            return Array.Empty<TDocument>();

        var db = _redis.GetDatabase(hashDescriptor.Database);
        var redisValue = db.HashGet(hashDescriptor.Key, new RedisValue(keyValue));
        if(redisValue.HasValue) {
            var document = JsonSerializer.Deserialize<TDocument>(redisValue);
            key.SetValue(document, Convert.ChangeType(keyValue, key.PropertyType));
            return new[] { document };
        }

        return Array.Empty<TDocument>();
    }

    private PropertyInfo GetKey(Type type) {
        var key = _propertyCache.GetOrAdd(type, _ => {
            var keys = TypeHelpers.GetAtlasKeyProperties(type);
            if(keys.Count() != 1)
                throw new ArgumentException("Only one AtlasKey is allowed for redis documents");

            return keys.First().Property;
        });
        return key;
    }
}