using System.Linq.Expressions;
using Fireflies.Atlas.Core;
using Fireflies.Atlas.Sources.SqlServer.Table;

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

    public static AtlasDocumentBuilder<TDocument> SqlServerSource<TDocument>(this AtlasDocumentBuilder<TDocument> builder, SqlServerSource source, string schema, string table, Expression<Func<TDocument, bool>> filter) where TDocument : new() {
        builder.AddSource(new SqlServerTableSource<TDocument>(source, new TableDescriptor(schema, table), filter));
        return builder;
    }
}