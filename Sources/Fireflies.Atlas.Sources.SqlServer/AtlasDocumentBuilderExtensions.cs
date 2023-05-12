using System.Linq.Expressions;
using Fireflies.Atlas.Core;

namespace Fireflies.Atlas.Sources.SqlServer;

public static class AtlasDocumentBuilderExtensions {
    public static AtlasDocumentBuilder<TDocument> SqlServerSource<TDocument>(this AtlasDocumentBuilder<TDocument> builder, SqlServerSource source, string schema, string table) where TDocument : new() {
        builder.AddSource(new SqlServerTableSource<TDocument>(source, new TableDescriptor(schema, table), null));
        return builder;
    }

    public static AtlasDocumentBuilder<TDocument> SqlServerSource<TDocument>(this AtlasDocumentBuilder<TDocument> builder, SqlServerSource source, string schema, string table, Expression<Func<TDocument, bool>> filter) where TDocument : new() {
        builder.AddSource(new SqlServerTableSource<TDocument>(source, new TableDescriptor(schema, table), filter));
        return builder;
    }
}