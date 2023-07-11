using Fireflies.Logging.Abstractions;
using System.Reflection.Emit;
using System.Reflection;
using Fireflies.Atlas.Core.Wrapper;

namespace Fireflies.Atlas.Core;

public class AtlasBuilder {
    private readonly List<AtlasDocumentBuilder> _builders = new();
    private readonly Atlas _atlas;
    private readonly ModuleBuilder _moduleBuilder;

    public static int _builderCounter = 0;
    private readonly WrapperGenerator _wrapperGenerator;

    public AtlasBuilder() {
        _atlas = new Atlas();
        var assemblyName = new AssemblyName($"Fireflies.Atlas.ProxyAssembly{_builderCounter++}");
        var dynamicAssembly = AssemblyBuilder.DefineDynamicAssembly(assemblyName, AssemblyBuilderAccess.Run);
        _moduleBuilder = dynamicAssembly.DefineDynamicModule("Main");
        _wrapperGenerator = new WrapperGenerator(_moduleBuilder);
    }

    public void SetLoggerFactory(IFirefliesLoggerFactory loggerFactory) {
        _atlas.LoggerFactory = loggerFactory;
    }

    public T Create<T>(Func<Atlas, T> Create) {
        var source = Create(_atlas);
        return source;
    }

    public AtlasDocumentBuilder<TDocument> AddDocument<TDocument>() where TDocument : new() {
        var builder = new AtlasDocumentBuilder<TDocument>(_atlas, _wrapperGenerator);
        _builders.Add(builder);
        return builder;
    }

    public async Task<Atlas> Build() {
        foreach(var builder in _builders) {
            if(builder.Source == null)
                throw new NullReferenceException($"Source for {builder.DocumentType.Name} must be set");

            await builder.PreBuild(_atlas).ConfigureAwait(false);
        }

        foreach(var builder in _builders) {
            await builder.PostBuild(_atlas).ConfigureAwait(false);
        }

        return _atlas;
    }
}