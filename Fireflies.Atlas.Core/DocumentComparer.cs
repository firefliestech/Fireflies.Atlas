using Fireflies.Atlas.Core.Helpers;

namespace Fireflies.Atlas.Core;

public static class DocumentComparer {
    public static bool Equals<TDocument>(TDocument first, TDocument second, bool ignoreType = false) {
        if(ReferenceEquals(null, second)) return false;
        if(ReferenceEquals(null, first)) return false;
        if(ReferenceEquals(first, second)) return true;
        if(!ignoreType && first.GetType() != second.GetType()) return false;

        // Calculate
        foreach(var property in TypeHelpers.GetAtlasProperties(typeof(TDocument))) {
            var thisValue = property.Property.GetValue(first);
            var otherValue = property.Property.GetValue(second);
            if(thisValue == null && otherValue == null)
                continue;
            if(thisValue == null)
                return false;
            if(otherValue == null)
                return false;

            if(!thisValue.Equals(otherValue))
                return false;
        }

        return true;
    }
}