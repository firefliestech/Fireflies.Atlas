namespace Fireflies.Atlas.Core;

[Flags]
public enum ExecutionFlags {
    None,
    BypassCache,
    DontCache,
    BypassFilter
}