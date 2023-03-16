using System.Reflection.Emit;
using System.Reflection;

namespace Fireflies.Atlas.Core.Wrapper;

internal class WrapperGenerator {
    private readonly ModuleBuilder _moduleBuilder;

    public WrapperGenerator(ModuleBuilder moduleBuilder) {
        _moduleBuilder = moduleBuilder;
    }

    public Func<TDocument, QueryContext, AtlasRelation<TDocument>[], TDocument> GenerateWrapper<TDocument>(AtlasRelation<TDocument>[] relations) {
        var baseType = typeof(TDocument);
        var typeBuilder = _moduleBuilder.DefineType($"{baseType.Name}Proxy",
            TypeAttributes.Public |
            TypeAttributes.Class |
            TypeAttributes.AutoClass |
            TypeAttributes.AnsiClass |
            TypeAttributes.BeforeFieldInit |
            TypeAttributes.AutoLayout,
            typeof(TDocument));

        foreach(var attribute in baseType.GetCustomAttributesData()) {
            typeBuilder.SetCustomAttribute(attribute.ToAttributeBuilder());
        }

        var instanceField = typeBuilder.DefineField("_instance", baseType, FieldAttributes.Private);
        var queryContextField = typeBuilder.DefineField("_queryContext", typeof(QueryContext), FieldAttributes.Private);
        var relationsField = typeBuilder.DefineField("_relations", typeof(AtlasRelation<TDocument>[]), FieldAttributes.Private);

        var constructorBuilder = typeBuilder.DefineConstructor(MethodAttributes.Public, CallingConventions.Standard, new[] { baseType, typeof(QueryContext), typeof(AtlasRelation<TDocument>[]) });
        var constructorIlGenerator = constructorBuilder.GetILGenerator();
        constructorIlGenerator.Emit(OpCodes.Ldarg_0);
        constructorIlGenerator.Emit(OpCodes.Ldarg_1);
        constructorIlGenerator.Emit(OpCodes.Stfld, instanceField);
        constructorIlGenerator.Emit(OpCodes.Ldarg_0);
        constructorIlGenerator.Emit(OpCodes.Ldarg_2);
        constructorIlGenerator.Emit(OpCodes.Stfld, queryContextField);
        constructorIlGenerator.Emit(OpCodes.Ldarg_0);
        constructorIlGenerator.Emit(OpCodes.Ldarg_3);
        constructorIlGenerator.Emit(OpCodes.Stfld, relationsField);
        constructorIlGenerator.Emit(OpCodes.Ret);

        foreach(var baseProperty in baseType.GetProperties(BindingFlags.Public | BindingFlags.Instance | BindingFlags.FlattenHierarchy | BindingFlags.GetProperty | BindingFlags.IgnoreCase).Where(p => p.DeclaringType != typeof(object))) {
            var propertyBuilder = typeBuilder.DefineProperty(baseProperty.Name, baseProperty.Attributes, CallingConventions.HasThis, baseProperty.PropertyType, Type.EmptyTypes);
            foreach(var attribute in baseProperty.GetCustomAttributesData())
                propertyBuilder.SetCustomAttribute(attribute.ToAttributeBuilder());

            var getMethod = typeBuilder.DefineMethod($"get_{baseProperty.Name}", MethodAttributes.Public | MethodAttributes.Virtual | MethodAttributes.SpecialName | MethodAttributes.HideBySig, CallingConventions.HasThis, baseProperty.PropertyType, Type.EmptyTypes);
            typeBuilder.DefineMethodOverride(getMethod, baseProperty.GetMethod!);
            propertyBuilder.SetGetMethod(getMethod);

            var relationIndex = relations.TakeWhile(t => t.Property != baseProperty).Count();
            if(relationIndex != relations.Length) {
                baseProperty.PropertyType.IsEnumerable(out var elementType);
                var relationHelperMethod = typeof(WrapperHelper).GetMethod(nameof(WrapperHelper.GetForeignDocument), BindingFlags.Static | BindingFlags.Public)!.MakeGenericMethod(typeof(TDocument), elementType);

                var getILGenerator = getMethod.GetILGenerator();
                getILGenerator.Emit(OpCodes.Ldarg_0);
                getILGenerator.Emit(OpCodes.Ldfld, relationsField);
                getILGenerator.Emit(OpCodes.Ldc_I4, relationIndex);
                getILGenerator.Emit(OpCodes.Ldelem_Ref);
                getILGenerator.Emit(OpCodes.Ldarg_0);
                getILGenerator.Emit(OpCodes.Ldfld, instanceField);
                getILGenerator.Emit(OpCodes.Ldarg_0);
                getILGenerator.Emit(OpCodes.Ldfld, queryContextField);
                getILGenerator.EmitCall(OpCodes.Call, relationHelperMethod, null);
                getILGenerator.Emit(OpCodes.Ret);
            } else {
                var getILGenerator = getMethod.GetILGenerator();
                getILGenerator.Emit(OpCodes.Ldarg_0);
                getILGenerator.Emit(OpCodes.Ldfld, instanceField);
                getILGenerator.EmitCall(OpCodes.Callvirt, baseProperty.GetMethod!, null);
                getILGenerator.Emit(OpCodes.Ret);
            }
        }

        var type = typeBuilder.CreateType()!;
        return (document, queryContext, r) => (TDocument)Activator.CreateInstance(type, document, queryContext, r)!;
    }
}

public static class WrapperHelper {
    public static object? GetForeignDocument<TDocument, TForeign>(AtlasRelation<TDocument, TForeign> relation, TDocument document, QueryContext queryContext) where TForeign : new() where TDocument : new() {
        return relation.GetForeignDocument(document, queryContext).Result;
    }
}

public static class EmitExtensions {
    public static CustomAttributeBuilder ToAttributeBuilder(this CustomAttributeData data) {
        if(data == null) {
            throw new ArgumentNullException(nameof(data));
        }

        var propertyArguments = new List<PropertyInfo>();
        var propertyArgumentValues = new List<object>();
        var fieldArguments = new List<FieldInfo>();
        var fieldArgumentValues = new List<object>();
        foreach(var namedArg in data.NamedArguments) {
            var fi = namedArg.MemberInfo as FieldInfo;
            var pi = namedArg.MemberInfo as PropertyInfo;

            if(fi != null) {
                fieldArguments.Add(fi);
                fieldArgumentValues.Add(namedArg.TypedValue.Value!);
            } else if(pi != null) {
                propertyArguments.Add(pi);
                propertyArgumentValues.Add(namedArg.TypedValue.Value!);
            }
        }

        return new CustomAttributeBuilder(data.Constructor,
            data.ConstructorArguments.Select(ctorArg => ctorArg.Value!).ToArray(),
            propertyArguments.ToArray(),
            propertyArgumentValues.ToArray(),
            fieldArguments.ToArray(),
            fieldArgumentValues.ToArray());
    }

    public static void CopyAttributes(this MemberInfo copyFrom, Action<CustomAttributeBuilder> callback, Func<Type, bool>? copyIf = null) {
        foreach(var customAttribute in copyFrom.GetCustomAttributesData()) {
            if(copyIf == null || copyIf(customAttribute.AttributeType))
                callback(customAttribute.ToAttributeBuilder());
        }
    }
}

public static class EnumerableExtensions {
    public static bool IsQueryable(this Type type, out Type elementType) {
        return GetElementTypeForEnumerableOf(type, out elementType, typeof(IQueryable<>), false);
    }

    public static bool IsQueryable(this Type type) {
        return type.IsQueryable(out _);
    }

    public static bool IsEnumerable(this Type type, out Type elementType) {
        return GetElementTypeForEnumerableOf(type, out elementType, typeof(IEnumerable<>), true);
    }

    private static bool GetElementTypeForEnumerableOf(Type type, out Type elementType, Type lookingFor, bool checkArray) {
        elementType = type.DiscardTask();

        if(elementType.IsGenericType) {
            var typeDefinition = elementType.GetGenericTypeDefinition();

            if(typeDefinition == lookingFor) {
                elementType = GetElementType(elementType);
                return true;
            }
        }

        if(checkArray && elementType.IsArray) {
            elementType = GetElementType(elementType);
            return true;
        }

        return false;
    }

    public static bool IsEnumerable(this Type type) {
        return type.IsEnumerable(out _);
    }

    public static bool IsCollection(this Type type, out Type elementType) {
        elementType = type.DiscardTask();

        if(type.IsEnumerable(out var enumerableType) || elementType.IsQueryable(out enumerableType)) {
            elementType = enumerableType;
            return true;
        }

        var implementICollection = type.GetInterfaces()
            .FirstOrDefault(x => x.IsGenericType && x.GetGenericTypeDefinition() == typeof(ICollection<>));

        if(implementICollection != null) {
            elementType = implementICollection.GetGenericArguments()[0];
            return true;
        }

        return false;
    }

    public static bool IsCollection(this Type type) {
        return type.IsCollection(out _);
    }

    private static Type GetElementType(this Type type) {
        return type.IsArray ? type.GetElementType()! : type.GetGenericArguments()[0];
    }

    public static Type DiscardTaskFromReturnType(this MethodInfo methodInfo) {
        return methodInfo.ReturnType.DiscardTask();
    }

    public static Type DiscardTask(this Type type) {
        if(type.IsGenericType && (type.GetGenericTypeDefinition() == typeof(Task<>) || type.GetGenericTypeDefinition() == typeof(IAsyncEnumerable<>)))
            return type.GetGenericArguments()[0];

        return type;
    }
}