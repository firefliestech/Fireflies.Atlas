namespace Fireflies.Atlas.Annotations;

public class AtlasFieldAttribute : AtlasAttribute {
    public AtlasFieldAttribute() {
    }

    public AtlasFieldAttribute(string? name) {
        Name = name;
    }

    public string? Name { get; set; }
}
