namespace Fireflies.Atlas.Core;

public class AtlasException : Exception {
    public AtlasException(string? message) : base(message) {
    }

    public AtlasException(string? message, Exception? innerException) : base(message, innerException) {
    }
}