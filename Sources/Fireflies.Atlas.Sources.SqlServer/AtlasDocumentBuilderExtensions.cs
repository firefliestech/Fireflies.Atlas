using Fireflies.Atlas.Core;

namespace Fireflies.Atlas.Sources.SqlServer;

public static class AtlasDocumentBuilderExtensions {
    public static AtlasDocumentBuilder<TDocument> SqlServerSource<TDocument>(this AtlasDocumentBuilder<TDocument> builder, SqlServerSource source, string schema, string table) where TDocument : new() {
        builder.AddSource(new SqlServerTableSource(source, new TableDescriptor { Schema = schema, Table = table }));
        return builder;
    }
}