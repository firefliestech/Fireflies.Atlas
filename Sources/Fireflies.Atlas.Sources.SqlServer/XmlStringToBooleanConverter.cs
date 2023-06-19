using System.Text.Json;

namespace Fireflies.Atlas.Sources.SqlServer;

public class XmlStringToBooleanConverter : System.Text.Json.Serialization.JsonConverter<object> {
    public override bool CanConvert(Type typeToConvert) {
        return typeToConvert == typeof(bool);
    }

    public override object? Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options) {
        var value = reader.GetString();
        if(value == null)
            return false;

        if(value == "1")
            return true;

        if(value == "0")
            return false;

        return bool.TryParse(value, out var boolValue) && boolValue;
    }

    public override void Write(Utf8JsonWriter writer, object value, JsonSerializerOptions options) {
    }
}