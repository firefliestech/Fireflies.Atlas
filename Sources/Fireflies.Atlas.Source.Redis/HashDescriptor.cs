using System.Reflection;

namespace Fireflies.Atlas.Source.Redis;

public class HashDescriptor {
    public int Database { get; set; }
    public string Key { get; set; }
    public PropertyInfo KeyProperty { get; set; }
    public PropertyInfo? ValueProperty { get; set; }
}