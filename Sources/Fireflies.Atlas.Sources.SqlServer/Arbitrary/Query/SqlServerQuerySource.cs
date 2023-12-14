namespace Fireflies.Atlas.Sources.SqlServer.Arbitrary.Query;

public class SqlServerQuerySource<TDocument>(Core.Atlas atlas, SqlServerSource source, SqlQueryDescriptor queryDescriptor, SqlServerArbitrarySourceBuilder<TDocument> builder)
    : SqlServerArbitrarySource<TDocument>(atlas, source, queryDescriptor, builder)
    where TDocument : new();