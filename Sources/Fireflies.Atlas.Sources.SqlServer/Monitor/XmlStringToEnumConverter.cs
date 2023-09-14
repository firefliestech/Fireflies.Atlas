using System.Text.Json;

namespace Fireflies.Atlas.Sources.SqlServer.Monitor;

public class XmlStringToEnumConverter : System.Text.Json.Serialization.JsonConverter<object> {
    public override bool CanConvert(Type typeToConvert) {
        return typeToConvert.IsEnum;
    }

    public override object? Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options) {
        var value = reader.GetString();
        if(value == null)
            return default;

        return int.TryParse(value, out var intValue) ?
            Enum.ToObject(typeToConvert, intValue) :
            Enum.Parse(typeToConvert, value);
    }

    public override void Write(Utf8JsonWriter writer, object value, JsonSerializerOptions options) {
    }
}