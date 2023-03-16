namespace Fireflies.Atlas.Core.Linq;

internal static class TypeSystem {
    internal static Type GetElementType(Type seqType) {
        var enumerableType = FindIEnumerable(seqType);
        return enumerableType == null ? seqType : enumerableType.GetGenericArguments()[0];
    }

    private static Type? FindIEnumerable(Type seqType) {
        if(seqType == typeof(string))
            return null;

        if(seqType.IsArray)
            return typeof(IEnumerable<>).MakeGenericType(seqType.GetElementType());

        if(seqType.IsGenericType) {
            foreach(var arg in seqType.GetGenericArguments()) {
                var enumerableType = typeof(IEnumerable<>).MakeGenericType(arg);
                if(enumerableType.IsAssignableFrom(seqType)) {
                    return enumerableType;
                }
            }
        }

        var interfaces = seqType.GetInterfaces();
        if(interfaces.Length > 0) {
            foreach(var interf in interfaces) {
                var enumerableType = FindIEnumerable(interf);
                if(enumerableType != null)
                    return enumerableType;
            }
        }

        if(seqType.BaseType != null && seqType.BaseType != typeof(object)) {
            return FindIEnumerable(seqType.BaseType);
        }

        return null;
    }
}