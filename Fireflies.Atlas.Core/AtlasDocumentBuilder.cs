using System.Linq.Expressions;
using Fireflies.Atlas.Core.Wrapper;

namespace Fireflies.Atlas.Core;

public abstract class AtlasDocumentBuilder {
    internal Type DocumentType { get; set; }

    public AtlasSource Source { get; protected set; }
    public bool Preload { get; protected set; }

    internal abstract Task PreBuild(Atlas atlas);
    internal abstract Task InternalPreload(Atlas atlas);
    internal abstract Task PostBuild(Atlas atlas);
}

public class AtlasDocumentBuilder<TDocument> : AtlasDocumentBuilder where TDocument : new() {
    private readonly Atlas _atlas;
    private readonly WrapperGenerator _wrapperGenerator;
    private readonly AtlasDocumentDictionary<TDocument> _dictionary;
    private readonly List<AtlasRelation<TDocument>> _relations = new();

    private Func<Atlas, AtlasDocumentDictionary<TDocument>, Task>? _beforePreload;
    private Func<Atlas, AtlasDocumentDictionary<TDocument>, Task>? _afterPreload;

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

    public AtlasDocumentBuilder<TDocument> BeforePreload(Func<Atlas, AtlasDocumentDictionary<TDocument>, Task>? callback) {
        _beforePreload = callback;
        return this;
    }

    public AtlasDocumentBuilder<TDocument> AfterPreload(Func<Atlas, AtlasDocumentDictionary<TDocument>, Task>? callback) {
        _afterPreload = callback;
        return this;
    }

    internal override Task PreBuild(Atlas atlas) {
        var relations = _relations.ToArray();
        var wrapperType = _wrapperGenerator.GenerateWrapper(relations);

        _dictionary.Relations = relations;
        _dictionary.ProxyFactory = wrapperType;

        return Task.CompletedTask;
    }

    internal override async Task InternalPreload(Atlas atlas) {
        if(!Preload)
            return;

        if(_beforePreload != null)
            await _beforePreload(_atlas, _dictionary).ConfigureAwait(false);

        await _dictionary.Preload().ConfigureAwait(false);

        if(_afterPreload != null)
            await _afterPreload(_atlas, _dictionary).ConfigureAwait(false);
    }

    internal override Task PostBuild(Atlas atlas) {
        return Task.CompletedTask;
    }

    public AtlasDocumentBuilder<TDocument> AddSource(AtlasSource<TDocument> source) {
        Source = source;
        _dictionary.Source = source;
        return this;
    }

    public AtlasDocumentBuilder<TDocument> OnUpdate(Action<(Atlas Atlas, TDocument NewDocument, TDocument? OldDocument)> action) {
        _dictionary.Updated += (newDocument, oldDocument) => action((_atlas, newDocument, oldDocument));
        return this;
    }

    public AtlasDocumentBuilder<TDocument> OnUpdate(Func<(Atlas Atlas, TDocument NewDocument, TDocument? OldDocument), Task> action) {
        _dictionary.Updated += async (newDocument, oldDocument) => await action((_atlas, newDocument, oldDocument)).ConfigureAwait(false);
        return this;
    }

    public AtlasDocumentBuilder<TDocument> OnLoad(Action<(Atlas Atlas, TDocument Document)> action) {
        _dictionary.Loaded += document => action((_atlas, document));
        return this;
    }

    public AtlasDocumentBuilder<TDocument> OnLoad(Func<(Atlas Atlas, TDocument Document), Task> action) {
        _dictionary.Loaded += async document => await action((_atlas, document)).ConfigureAwait(false);
        return this;
    }
}