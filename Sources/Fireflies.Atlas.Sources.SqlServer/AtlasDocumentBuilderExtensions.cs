using System.Linq.Expressions;
using Fireflies.Atlas.Core;
using Fireflies.Atlas.Sources.SqlServer.Table;
using Fireflies.Atlas.Sources.SqlServer.View;

namespace Fireflies.Atlas.Sources.SqlServer;

public static class AtlasDocumentBuilderExtensions {
    public static AtlasDocumentBuilder<TDocument> SqlServerTableSource<TDocument>(this AtlasDocumentBuilder<TDocument> builder, SqlServerSource source, string descriptor, Expression<Func<TDocument, bool>>? filter = null) where TDocument : new() {
        return builder.SqlServerTableSource(source, new SqlDescriptor(descriptor), filter);
    }

    public static AtlasDocumentBuilder<TDocument> SqlServerTableSource<TDocument>(this AtlasDocumentBuilder<TDocument> builder, SqlServerSource source, string schema, string table, Expression<Func<TDocument, bool>>? filter = null) where TDocument : new() {
        return builder.SqlServerTableSource(source, new SqlDescriptor(schema, table), filter);
    }

    public static AtlasDocumentBuilder<TDocument> SqlServerTableSource<TDocument>(this AtlasDocumentBuilder<TDocument> builder, SqlServerSource source, SqlDescriptor descriptor, Expression<Func<TDocument, bool>>? filter = null) where TDocument : new() {
        builder.AddSource(new SqlServerTableSource<TDocument>(source.Atlas, source, descriptor, filter));
        return builder;
    }

    public static AtlasDocumentBuilder<TDocument> SqlServerViewSource<TDocument>(this AtlasDocumentBuilder<TDocument> builder, SqlServerSource source, string descriptor, Action<SqlServerViewSourceBuilder<TDocument>> configure, Expression<Func<TDocument, bool>>? filter = null) where TDocument : new() {
        return builder.SqlServerViewSource(source, new SqlDescriptor(descriptor), configure, filter);
    }

    public static AtlasDocumentBuilder<TDocument> SqlServerViewSource<TDocument>(this AtlasDocumentBuilder<TDocument> builder, SqlServerSource source, SqlDescriptor viewDescriptor, Action<SqlServerViewSourceBuilder<TDocument>> configure, Expression<Func<TDocument, bool>>? filter = null) where TDocument : new() {
        var sqlServerViewSourceBuilder = new SqlServerViewSourceBuilder<TDocument>();
        configure(sqlServerViewSourceBuilder);
        builder.AddSource(new SqlServerViewSource<TDocument>(source.Atlas, source, viewDescriptor, sqlServerViewSourceBuilder.TableTriggerBuilders.Select(x => x.Build()), filter));
        return builder;
    }
}