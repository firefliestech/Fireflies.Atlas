using System.Collections.Concurrent;
using System.Linq.Expressions;
using System.Reflection;
using System.Text.Json;
using System.Text.Json.Serialization;
using Fireflies.Atlas.Core;
using StackExchange.Redis;

namespace Fireflies.Atlas.Source.Redis;

public class RedisSource {
    private readonly Core.Atlas _atlas;
    private readonly ConnectionMultiplexer _redis;
    private readonly ConcurrentDictionary<Type, PropertyInfo> _propertyCache = new();
    private readonly JsonSerializerOptions _serializerOptions = new() { PropertyNameCaseInsensitive = true };

    public RedisSource(Core.Atlas atlas, string connectionString, params JsonConverter[] converters) {
        _atlas = atlas;
        _redis = ConnectionMultiplexer.Connect(connectionString);

        foreach(var converter in converters) {
            _serializerOptions.Converters.Add(converter);
        }
    }

    public async Task<IEnumerable<(bool Cache, TDocument Document)>> GetHashDocuments<TDocument>(Expression<Func<TDocument, bool>>? predicate, HashDescriptor hashDescriptor) where TDocument : new() {
        var queryDocument = PredicateToDocument.CreateDocument(predicate);
        var keyValue = hashDescriptor.KeyProperty.GetMethod!.Invoke(queryDocument, Array.Empty<object>())?.ToString();
        if(keyValue == null)
            return Array.Empty<(bool, TDocument)>();

        var db = _redis.GetDatabase(hashDescriptor.Database);
        var redisValue = await db.HashGetAsync(hashDescriptor.Key, new RedisValue(keyValue)).ConfigureAwait(false);
        if(redisValue.HasValue) {
            if(hashDescriptor.ValueProperty != null) {
                var document = Activator.CreateInstance<TDocument>();
                hashDescriptor.KeyProperty.SetValue(document, Convert.ChangeType(keyValue, hashDescriptor.KeyProperty.PropertyType));
                hashDescriptor.ValueProperty.SetValue(document, JsonSerializer.Deserialize(redisValue!, hashDescriptor.ValueProperty.PropertyType, _serializerOptions)!);
                return new[] { (false, document) };
            } else {
                var document = JsonSerializer.Deserialize<TDocument>(redisValue!, _serializerOptions)!;
                hashDescriptor.KeyProperty.SetValue(document, Convert.ChangeType(keyValue, hashDescriptor.KeyProperty.PropertyType));
                return new[] { (false, document) };
            }
        }

        return Array.Empty<(bool, TDocument)>();
    }

    public async Task<IEnumerable<(bool Cache, TDocument Document)>> GetKeyDocuments<TDocument>(Expression<Func<TDocument, bool>> predicate, KeyDescriptor keyDescriptor) where TDocument : new() {
        var queryDocument = PredicateToDocument.CreateDocument(predicate);
        var keyValue = keyDescriptor.KeyProperty.GetMethod!.Invoke(queryDocument, Array.Empty<object>())?.ToString();
        if(keyValue == null)
            return Array.Empty<(bool, TDocument)>();

        var db = _redis.GetDatabase(keyDescriptor.Database);
        var redisValue = await db.StringGetAsync(keyDescriptor.KeyBuilder(keyValue)).ConfigureAwait(false);
        if(redisValue.HasValue) {
            var document = Activator.CreateInstance<TDocument>();
            
            keyDescriptor.KeyProperty.SetValue(document, Convert.ChangeType(keyValue, keyDescriptor.KeyProperty.PropertyType));

            var value = JsonSerializer.Deserialize(redisValue!, keyDescriptor.ValueProperty.PropertyType, _serializerOptions)!;
            keyDescriptor.ValueProperty.SetValue(document, value);
            
            return new[] { (false, document) };
        }

        return Array.Empty<(bool, TDocument)>();
    }
}