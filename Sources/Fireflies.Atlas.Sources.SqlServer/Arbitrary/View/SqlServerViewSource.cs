namespace Fireflies.Atlas.Sources.SqlServer.Arbitrary.View;

public class SqlServerViewSource<TDocument>(Core.Atlas atlas, SqlServerSource source, SqlDescriptor viewDescriptor, SqlServerArbitrarySourceBuilder<TDocument> builder)
    : SqlServerArbitrarySource<TDocument>(atlas, source, viewDescriptor, builder)
    where TDocument : class, new();