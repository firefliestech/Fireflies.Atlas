using System.Linq.Expressions;
using Dapper;
using Fireflies.Atlas.Core;
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

    public async Task<IEnumerable<TDocument>> GetDocuments<TDocument>(Expression<Func<TDocument, bool>>? predicate, TableDescriptor tableDescriptor, Expression<Func<TDocument, bool>>? filter, ExecutionFlags flags) where TDocument : new() {
        var documents = await InternalGetDocuments(predicate, tableDescriptor, filter, flags).ConfigureAwait(false);
        return documents;
    }

    private async Task<IEnumerable<TDocument>> InternalGetDocuments<TDocument>(Expression<Func<TDocument, bool>>? predicate, TableDescriptor tableDescriptor, Expression<Func<TDocument, bool>>? filter, ExecutionFlags flags) where TDocument : new() {
        _updateFetcher.MonitorTable(tableDescriptor, filter);
        
        using var lambdaToSqlTranslator = new LambdaToSqlTranslator<TDocument>();
        var query = lambdaToSqlTranslator.Translate(tableDescriptor, predicate, flags.HasFlag(ExecutionFlags.BypassFilter) ? null : filter);
        await using var connection = new SqlConnection(_connectionString);
        connection.Open();
        return (await connection.QueryAsync<TDocument>(query).ConfigureAwait(false)).ToArray();
    }

    public void Dispose() {
        _updateFetcher.Dispose();
    }
}