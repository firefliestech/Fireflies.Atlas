using Fireflies.Atlas.Core;
using Fireflies.Atlas.Sources.SqlServer.Table;
using Fireflies.Atlas.Sources.SqlServer.View;

namespace Fireflies.Atlas.Sources.SqlServer;

public static class AtlasDocumentBuilderExtensions {
    public static AtlasDocumentBuilder<TDocument> SqlServerTableSource<TDocument>(this AtlasDocumentBuilder<TDocument> builder, SqlServerSource source, string descriptor, Action<SqlServerTableSourceBuilder<TDocument>>? configure = null) where TDocument : new() {
        return builder.SqlServerTableSource(source, new SqlDescriptor(descriptor), configure);
    }

    public static AtlasDocumentBuilder<TDocument> SqlServerTableSource<TDocument>(this AtlasDocumentBuilder<TDocument> builder, SqlServerSource source, string schema, string table, Action<SqlServerTableSourceBuilder<TDocument>>? configure = null) where TDocument : new() {
        return builder.SqlServerTableSource(source, new SqlDescriptor(schema, table), configure);
    }

    public static AtlasDocumentBuilder<TDocument> SqlServerTableSource<TDocument>(this AtlasDocumentBuilder<TDocument> builder, SqlServerSource source, SqlDescriptor descriptor, Action<SqlServerTableSourceBuilder<TDocument>>? configure = null) where TDocument : new() {
        var tableSourceBuilder = new SqlServerTableSourceBuilder<TDocument>();
        configure?.Invoke(tableSourceBuilder);
        builder.AddSource(new SqlServerTableSource<TDocument>(source.Atlas, source, descriptor, tableSourceBuilder));
        return builder;
    }

    public static AtlasDocumentBuilder<TDocument> SqlServerViewSource<TDocument>(this AtlasDocumentBuilder<TDocument> builder, SqlServerSource source, string descriptor, Action<SqlServerViewSourceBuilder<TDocument>> configure) where TDocument : new() {
        return builder.SqlServerViewSource(source, new SqlDescriptor(descriptor), configure);
    }

    public static AtlasDocumentBuilder<TDocument> SqlServerViewSource<TDocument>(this AtlasDocumentBuilder<TDocument> builder, SqlServerSource source, string schema, string table, Action<SqlServerViewSourceBuilder<TDocument>> configure) where TDocument : new() {
        return builder.SqlServerViewSource(source, new SqlDescriptor(schema, table), configure);
    }

    public static AtlasDocumentBuilder<TDocument> SqlServerViewSource<TDocument>(this AtlasDocumentBuilder<TDocument> builder, SqlServerSource source, SqlDescriptor viewDescriptor, Action<SqlServerViewSourceBuilder<TDocument>> configure) where TDocument : new() {
        var sqlServerViewSourceBuilder = new SqlServerViewSourceBuilder<TDocument>();
        configure(sqlServerViewSourceBuilder);
        builder.AddSource(new SqlServerViewSource<TDocument>(source.Atlas, source, viewDescriptor, sqlServerViewSourceBuilder));
        return builder;
    }
}