using Fireflies.Atlas.Core;
using Fireflies.Atlas.Sources.SqlServer.Arbitrary;
using Fireflies.Atlas.Sources.SqlServer.Arbitrary.Query;
using Fireflies.Atlas.Sources.SqlServer.Arbitrary.View;
using Fireflies.Atlas.Sources.SqlServer.Table;

namespace Fireflies.Atlas.Sources.SqlServer;

public static class AtlasDocumentBuilderExtensions {
    public static AtlasDocumentBuilder<TDocument> SqlServerTableSource<TDocument>(this AtlasDocumentBuilder<TDocument> builder, SqlServerSource source, string descriptor, Action<SqlServerTableSourceBuilder<TDocument>>? configure = null) where TDocument : class, new() {
        return builder.SqlServerTableSource(source, new SqlNameDescriptor(descriptor), configure);
    }

    public static AtlasDocumentBuilder<TDocument> SqlServerTableSource<TDocument>(this AtlasDocumentBuilder<TDocument> builder, SqlServerSource source, string schema, string table, Action<SqlServerTableSourceBuilder<TDocument>>? configure = null) where TDocument : class, new() {
        return builder.SqlServerTableSource(source, new SqlNameDescriptor(schema, table), configure);
    }

    public static AtlasDocumentBuilder<TDocument> SqlServerTableSource<TDocument>(this AtlasDocumentBuilder<TDocument> builder, SqlServerSource source, SqlNameDescriptor descriptor, Action<SqlServerTableSourceBuilder<TDocument>>? configure = null) where TDocument : class, new() {
        var tableSourceBuilder = new SqlServerTableSourceBuilder<TDocument>();
        configure?.Invoke(tableSourceBuilder);
        builder.AddSource(new SqlServerTableSource<TDocument>(source.Atlas, source, descriptor, tableSourceBuilder));
        return builder;
    }

    public static AtlasDocumentBuilder<TDocument> SqlServerViewSource<TDocument>(this AtlasDocumentBuilder<TDocument> builder, SqlServerSource source, string descriptor, Action<SqlServerArbitrarySourceBuilder<TDocument>> configure) where TDocument : class, new() {
        return builder.SqlServerViewSource(source, new SqlNameDescriptor(descriptor), configure);
    }

    public static AtlasDocumentBuilder<TDocument> SqlServerViewSource<TDocument>(this AtlasDocumentBuilder<TDocument> builder, SqlServerSource source, string schema, string table, Action<SqlServerArbitrarySourceBuilder<TDocument>> configure) where TDocument : class, new() {
        return builder.SqlServerViewSource(source, new SqlNameDescriptor(schema, table), configure);
    }

    public static AtlasDocumentBuilder<TDocument> SqlServerViewSource<TDocument>(this AtlasDocumentBuilder<TDocument> builder, SqlServerSource source, SqlNameDescriptor viewDescriptor, Action<SqlServerArbitrarySourceBuilder<TDocument>> configure) where TDocument : class, new() {
        var sqlServerViewSourceBuilder = new SqlServerArbitrarySourceBuilder<TDocument>();
        configure(sqlServerViewSourceBuilder);
        builder.AddSource(new SqlServerViewSource<TDocument>(source.Atlas, source, viewDescriptor, sqlServerViewSourceBuilder));
        return builder;
    }

    public static AtlasDocumentBuilder<TDocument> SqlServerQuerySource<TDocument>(this AtlasDocumentBuilder<TDocument> builder, SqlServerSource source, string query, Action<SqlServerArbitrarySourceBuilder<TDocument>> configure) where TDocument : class, new() {
        return builder.SqlServerQuerySource(source, new SqlQueryDescriptor(query), configure);
    }

    public static AtlasDocumentBuilder<TDocument> SqlServerQuerySource<TDocument>(this AtlasDocumentBuilder<TDocument> builder, SqlServerSource source, SqlQueryDescriptor queryDescriptor, Action<SqlServerArbitrarySourceBuilder<TDocument>> configure) where TDocument : class, new() {
        var sqlServerViewSourceBuilder = new SqlServerArbitrarySourceBuilder<TDocument>();
        configure(sqlServerViewSourceBuilder);
        builder.AddSource(new SqlServerQuerySource<TDocument>(source.Atlas, source, queryDescriptor, sqlServerViewSourceBuilder));
        return builder;
    }
}