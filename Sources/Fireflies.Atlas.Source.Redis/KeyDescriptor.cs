using System.Reflection;

namespace Fireflies.Atlas.Source.Redis;

public class KeyDescriptor {
    public int Database { get; set; }
    public Func<string, string> KeyBuilder { get; set; }
    public PropertyInfo KeyProperty { get; set; }
    public PropertyInfo ValueProperty { get; set; }
}