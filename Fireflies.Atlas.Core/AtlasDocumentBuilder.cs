using System.Linq.Expressions;
using Fireflies.Atlas.Core.Wrapper;

namespace Fireflies.Atlas.Core;

public abstract class AtlasDocumentBuilder {
    internal AtlasSource Source { get; set; }
    internal Type DocumentType { get; set; }

    public bool Preload { get; internal set; }
    internal abstract Task PreBuild(Atlas atlas);
    internal abstract Task PostBuild(Atlas atlas);
}

public class AtlasDocumentBuilder<TDocument> : AtlasDocumentBuilder where TDocument : new() {
    private readonly Atlas _atlas;
    private readonly WrapperGenerator _wrapperGenerator;
    private readonly AtlasDocumentDictionary<TDocument> _dictionary;
    private readonly List<AtlasRelation<TDocument>> _relations = new();

    internal AtlasDocumentBuilder(Atlas atlas, WrapperGenerator wrapperGenerator) {
        _atlas = atlas;
        _wrapperGenerator = wrapperGenerator;
        _dictionary = atlas.Add<TDocument>();
        DocumentType = typeof(TDocument);
    }

    public AtlasDocumentBuilder<TDocument> AddRelation<TForeign>(Expression<Func<TDocument, object>> property, Expression<Func<TDocument, TForeign, bool>> matchExpression) where TForeign : new() {
        _relations.Add(new AtlasRelation<TDocument, TForeign>(_atlas, property, matchExpression));
        return this;
    }

    public AtlasDocumentBuilder<TDocument> AddIndex<TProperty>(Expression<Func<TDocument, TProperty>> property) {
        _dictionary.AddIndex(property);
        return this;
    }

    public AtlasDocumentBuilder<TDocument> PreloadDocuments() {
        Preload = true;
        return this;
    }

    internal override Task PreBuild(Atlas atlas) {
        var relations = _relations.ToArray();
        var wrapperType = _wrapperGenerator.GenerateWrapper(relations);

        _dictionary.Relations = relations;
        _dictionary.ProxyFactory = wrapperType;

        return Preload ? _dictionary.Preload() : Task.CompletedTask;
    }

    internal override Task PostBuild(Atlas atlas) {
        return Task.CompletedTask;
    }

    public AtlasDocumentBuilder<TDocument> AddSource(AtlasSource source) {
        Source = source;
        _dictionary.Source = source;
        return this;
    }
}