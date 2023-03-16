using System.Linq.Expressions;
using Dapper;
using Fireflies.Logging.Abstractions;
using Microsoft.Data.SqlClient;

namespace Fireflies.Atlas.Sources.SqlServer;

public class SqlServerSource : IDisposable {
    private readonly Core.Atlas _atlas;

    private readonly string _connectionString;
    private readonly SqlMonitor _updateFetcher;
    private readonly IFirefliesLogger _logger;

    public SqlServerSource(Core.Atlas atlas, string connectionString) {
        _atlas = atlas;
        _connectionString = connectionString;
        _logger = _atlas.LoggerFactory.GetLogger<SqlServerSource>();
        _updateFetcher = new SqlMonitor(_connectionString, atlas);
    }

    public async Task<(bool Cache, IEnumerable<TDocument> Documents)> GetDocuments<TDocument>(Expression<Func<TDocument, bool>>? predicate, TableDescriptor tableDescriptor) where TDocument : new() {
        return (true, await InternalGetDocuments(predicate, tableDescriptor));
    }

    private async Task<IEnumerable<TDocument>> InternalGetDocuments<TDocument>(Expression<Func<TDocument, bool>>? predicate, TableDescriptor tableDescriptor) where TDocument : new() {
        _updateFetcher.MonitorTable<TDocument>(tableDescriptor);
        using var lambdaToSqlTranslator = new LambdaToSqlTranslator<TDocument>();
        var query = lambdaToSqlTranslator.Translate(tableDescriptor, predicate);
        await using var connection = new SqlConnection(_connectionString);
        connection.Open();
        return (await connection.QueryAsync<TDocument>(query)).ToArray();
    }

    public void Dispose() {
        _updateFetcher.Dispose();
    }
}